module.exports = {
  apps: [{
    name: "coda-helper",
    script: "run.py",
    interpreter: "./venv/bin/python",
    instances: 1,
    exec_mode: "fork",
    watch: true,
    env: {
      PYTHONPATH: "."
    }
  }]
}
