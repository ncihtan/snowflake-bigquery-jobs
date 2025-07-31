import yaml
import subprocess

with open("secrets.yaml", "r") as f:
    secrets = yaml.safe_load(f)
    print("Loaded secrets from secrets.yaml")
    print(secrets)

for key, value in secrets.items():
    if key == "GOOGLE_CLOUD_PROJECT":
        continue  # skip non-secrets

    print(f"Creating secret: {key.lower().replace('_', '-')}")
    secret_name = key.lower().replace("_", "-")

    # Create the secret
    subprocess.run(
        ["gcloud", "secrets", "create", secret_name, "--data-file=-"],
        input=value.encode("utf-8"),
        check=True,
    )
