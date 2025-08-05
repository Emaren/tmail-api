module.exports = {
    apps: [
      {
        name: "tmail-api",
        script: "/var/www/tmail-api/track_server.py",
        interpreter: "/var/www/tmail-api/.direnv/python-3.13.5/bin/python",
        watch: true,
        env: {
          PORT: 8009,
          HOST: "0.0.0.0",
        },
      },
    ],
  };
  