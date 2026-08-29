module.exports = {
  apps: [
    {
      name: "intake_sidecar",
      script: "src/cli.py",
      args: "intake",
      interpreter: "python3",
      cwd: "/Users/solveetcoagula/Desktop/activeProjects/universal_bounty_swarm",
      autorestart: true,
      max_restarts: 10,
      restart_delay: 2000,
      watch: false,
      env: {
        PYTHONUNBUFFERED: "1",
        GOOGLE_APPLICATION_CREDENTIALS: "/Users/solveetcoagula/Desktop/activeProjects/bounty_operations/.agents/credentials.json",
        DOCKER_HOST: "unix:///Users/solveetcoagula/.orbstack/run/docker.sock",
        GCP_PROJECT_ID: "odin-500008",
        PYTHONPATH: "."
      },
      error_file: "logs/pm2-intake-error.log",
      out_file: "logs/pm2-intake-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z"
    },
    {
      name: "executor_sidecar",
      script: "src/cli.py",
      args: "executor",
      interpreter: "python3",
      cwd: "/Users/solveetcoagula/Desktop/activeProjects/universal_bounty_swarm",
      autorestart: true,
      max_restarts: 10,
      restart_delay: 2000,
      watch: false,
      env: {
        PYTHONUNBUFFERED: "1",
        GOOGLE_APPLICATION_CREDENTIALS: "/Users/solveetcoagula/Desktop/activeProjects/bounty_operations/.agents/credentials.json",
        DOCKER_HOST: "unix:///Users/solveetcoagula/.orbstack/run/docker.sock",
        GCP_PROJECT_ID: "odin-500008",
        PYTHONPATH: "."
      },
      error_file: "logs/pm2-executor-error.log",
      out_file: "logs/pm2-executor-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z"
    },
    {
      name: "escort_sidecar",
      script: "src/cli.py",
      args: "escort",
      interpreter: "python3",
      cwd: "/Users/solveetcoagula/Desktop/activeProjects/universal_bounty_swarm",
      autorestart: true,
      max_restarts: 10,
      restart_delay: 2000,
      watch: false,
      env: {
        PYTHONUNBUFFERED: "1",
        GOOGLE_APPLICATION_CREDENTIALS: "/Users/solveetcoagula/Desktop/activeProjects/bounty_operations/.agents/credentials.json",
        DOCKER_HOST: "unix:///Users/solveetcoagula/.orbstack/run/docker.sock",
        GCP_PROJECT_ID: "odin-500008",
        PYTHONPATH: "."
      },
      error_file: "logs/pm2-escort-error.log",
      out_file: "logs/pm2-escort-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z"
    },
    {
      name: "sync_sidecar",
      script: "src/cli.py",
      args: "sync",
      interpreter: "python3",
      cwd: "/Users/solveetcoagula/Desktop/activeProjects/universal_bounty_swarm",
      autorestart: true,
      max_restarts: 10,
      restart_delay: 2000,
      watch: false,
      env: {
        PYTHONUNBUFFERED: "1",
        GOOGLE_APPLICATION_CREDENTIALS: "/Users/solveetcoagula/Desktop/activeProjects/bounty_operations/.agents/credentials.json",
        DOCKER_HOST: "unix:///Users/solveetcoagula/.orbstack/run/docker.sock",
        GCP_PROJECT_ID: "odin-500008",
        PYTHONPATH: "."
      },
      error_file: "logs/pm2-sync-error.log",
      out_file: "logs/pm2-sync-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z"
    }
  ]
};
