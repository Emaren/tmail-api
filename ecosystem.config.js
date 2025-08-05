module.exports = {
  apps: [
    {
      name: "track_server",
      script: "track_server.py",
      interpreter: "/var/www/tmail-api/.direnv/python-3.13.5/bin/python",
      cwd: __dirname,
      watch: false,
      env: {
        HOST: "0.0.0.0",
        PORT: 8010,
      },
    },
    {
      name: "tmail-app",
      script: "npm",
      args: "run start",
      cwd: "/var/www/tmail-app",
      env: {
        NODE_ENV: "production",
        PORT: 3009,
        HOSTNAME: "0.0.0.0",
      },
    },
  ],
};
