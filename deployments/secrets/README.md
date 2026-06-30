# Secret Management

This directory contains runtime secrets injected into the Docker containers using Docker Compose secrets.

> [!WARNING]  
> Never commit these files to version control! The `.gitignore` at the project root should explicitly ignore `deployments/secrets/*.txt`.

## Production Hardening
In a true production environment, static files on the host are discouraged. Consider one of the following approaches:
1. **Host Security:** Apply strict file permissions. On Linux, ensure `chmod 600 *.txt` is set so only the root/docker user can read these files.
2. **Docker Swarm Secrets:** Use `docker secret create` and let Docker Swarm manage the encrypted secrets instead of relying on the host filesystem.
3. **Vault Agent:** Use HashiCorp Vault Agent or a similar tool to dynamically render these secret files into a `tmpfs` RAM disk before `docker compose up` is executed.
