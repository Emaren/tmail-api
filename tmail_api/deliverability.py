from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from typing import Iterable

from tmail_api.db import utc_now


APPLE_MANAGED_DOMAINS = {"icloud.com", "me.com", "mac.com"}
COMMON_DKIM_SELECTORS = ("sig1", "sig2", "default", "selector1", "selector2", "k1")


@dataclass
class DomainDiagnostics:
    domain: str
    spf: str
    dkim: str
    dmarc: str
    mx: str
    readiness: str
    notes: str
    last_checked_at: str


class DeliverabilityService:
    def list_domains(self, domains: Iterable[str]) -> list[dict[str, str]]:
        seen: set[str] = set()
        items: list[dict[str, str]] = []
        for domain in domains:
            normalized = domain.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            items.append(self.inspect_domain(normalized))
        return items

    def inspect_domain(self, domain: str) -> dict[str, str]:
        if not self._has_dns_tool():
            diagnostics = DomainDiagnostics(
                domain=domain,
                spf="warn",
                dkim="warn",
                dmarc="warn",
                mx="warn",
                readiness="DNS tooling unavailable in the local environment. Manual verification required.",
                notes="Install dig/nslookup tooling or run these checks on the VPS to verify DNS records.",
                last_checked_at=utc_now(),
            )
            return {
                "domain": diagnostics.domain,
                "spf": diagnostics.spf,
                "dkim": diagnostics.dkim,
                "dmarc": diagnostics.dmarc,
                "mx": diagnostics.mx,
                "readiness": diagnostics.readiness,
                "notes": diagnostics.notes,
                "last_checked_at": diagnostics.last_checked_at,
            }

        txt_root = self._lookup(domain, "TXT")
        mx_records = self._lookup(domain, "MX")
        dmarc_records = self._lookup(f"_dmarc.{domain}", "TXT")

        spf_records = [record for record in txt_root if record.lower().startswith("v=spf1")]
        dmarc_hits = [record for record in dmarc_records if record.lower().startswith("v=dmarc1")]
        dkim_records = self._lookup_dkim(domain)

        spf_status = "pass" if spf_records else "fail"
        dmarc_status = "pass" if dmarc_hits else "fail"
        mx_status = "pass" if mx_records else "fail"

        if dkim_records:
            dkim_status = "pass"
        elif domain in APPLE_MANAGED_DOMAINS:
            dkim_status = "warn"
        else:
            dkim_status = "fail"

        diagnostics = DomainDiagnostics(
            domain=domain,
            spf=spf_status,
            dkim=dkim_status,
            dmarc=dmarc_status,
            mx=mx_status,
            readiness=self._readiness(spf_status, dkim_status, dmarc_status, mx_status),
            notes=self._notes(
                domain=domain,
                spf_records=spf_records,
                dmarc_hits=dmarc_hits,
                mx_records=mx_records,
                dkim_records=dkim_records,
            ),
            last_checked_at=utc_now(),
        )
        return {
            "domain": diagnostics.domain,
            "spf": diagnostics.spf,
            "dkim": diagnostics.dkim,
            "dmarc": diagnostics.dmarc,
            "mx": diagnostics.mx,
            "readiness": diagnostics.readiness,
            "notes": diagnostics.notes,
            "last_checked_at": diagnostics.last_checked_at,
        }

    def _lookup_dkim(self, domain: str) -> list[str]:
        records: list[str] = []
        for selector in COMMON_DKIM_SELECTORS:
            name = f"{selector}._domainkey.{domain}"
            for record_type in ("CNAME", "TXT"):
                values = self._lookup(name, record_type)
                if values:
                    records.extend([f"{selector}:{value}" for value in values])
        return records

    def _lookup(self, name: str, record_type: str) -> list[str]:
        dig_path = shutil.which("dig")
        if not dig_path:
            return []

        try:
            result = subprocess.run(
                [dig_path, "+short", name, record_type],
                capture_output=True,
                check=False,
                text=True,
                timeout=6,
            )
        except (OSError, subprocess.TimeoutExpired):
            return []

        values: list[str] = []
        for line in result.stdout.splitlines():
            normalized = line.strip().strip('"')
            if normalized:
                values.append(normalized)
        return values

    def _has_dns_tool(self) -> bool:
        return bool(shutil.which("dig"))

    def _readiness(self, spf: str, dkim: str, dmarc: str, mx: str) -> str:
        statuses = [spf, dkim, dmarc, mx]
        if "fail" in statuses:
            return "Authentication gaps detected. Fix before meaningful sends."
        if "warn" in statuses:
            return "Baseline is usable, but one or more checks need manual confirmation."
        return "Healthy authentication baseline for tracked sends."

    def _notes(
        self,
        *,
        domain: str,
        spf_records: list[str],
        dmarc_hits: list[str],
        mx_records: list[str],
        dkim_records: list[str],
    ) -> str:
        notes: list[str] = []
        if spf_records:
            notes.append(f"SPF present ({spf_records[0][:90]})")
        else:
            notes.append("SPF record missing")

        if dmarc_hits:
            notes.append(f"DMARC present ({dmarc_hits[0][:90]})")
        else:
            notes.append("DMARC record missing")

        if mx_records:
            notes.append(f"MX present ({mx_records[0]})")
        else:
            notes.append("MX record missing")

        if dkim_records:
            notes.append(f"DKIM selector detected ({dkim_records[0][:90]})")
        elif domain in APPLE_MANAGED_DOMAINS:
            notes.append("Apple-managed domain; DKIM is provider-controlled and selector discovery is partial")
        else:
            notes.append("No common DKIM selector detected")

        return ". ".join(notes) + "."
