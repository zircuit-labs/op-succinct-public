Make sure you have sp1 tools installed and take care to match your local sp1
version with the one in the `Dockerfile`. To setup the SP1 toolchain run:

```bash
cd op-succinct/programs
sp1up --version 5.0.8
```

To build:

- Make sure you have the github token in your `GITHUB_TOKEN` environment
  variable
- `docker build --no-cache --platform=linux/amd64 --secret id=github_token,env=GITHUB_TOKEN -t zircuit-builder:latest .`
- `SP1_DOCKER_IMAGE=zircuit-builder:latest cargo prove build --docker`
