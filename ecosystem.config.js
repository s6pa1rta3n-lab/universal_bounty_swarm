module.exports = {
  apps: [
    {
      name: "intake_sidecar",
      script: "src/sidecars/intake_sidecar.py",
      interpreter: "python3",
      autorestart: true,
      env: { PYTHONUNBUFFERED: "1" }
    },
    {
      name: "executor_sidecar",
      script: "src/sidecars/executor_sidecar.py",
      interpreter: "python3",
      autorestart: true,
      env: { PYTHONUNBUFFERED: "1" }
    },
    {
      name: "escort_sidecar",
      script: "src/sidecars/escort_sidecar.py",
      interpreter: "python3",
      autorestart: true,
      env: { PYTHONUNBUFFERED: "1" }
    }
  ]
};
