import os

from tmail_api.factory import create_app

app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8010))
    host = os.getenv("HOST", "0.0.0.0")
    print(f"Listening on http://{host}:{port}")
    app.run(host=host, port=port)
