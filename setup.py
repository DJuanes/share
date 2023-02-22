from pathlib import Path

from setuptools import find_namespace_packages, setup

# Cargar paquetes desde requirements.txt
BASE_DIR = Path(__file__).parent
with open(Path(BASE_DIR, "requirements.txt"), "r") as file:
    required_packages = [ln.strip() for ln in file.readlines()]

with open(Path(BASE_DIR, "requirements-docs.txt"), "r") as file:
    docs_packages = [ln.strip() for ln in file.readlines()]

with open(Path(BASE_DIR, "requirements-quality.txt"), "r") as file:
    quality_packages = [ln.strip() for ln in file.readlines()]

with open(Path(BASE_DIR, "requirements-security.txt"), "r") as file:
    security_packages = [ln.strip() for ln in file.readlines()]

with open(Path(BASE_DIR, "requirements-test.txt"), "r") as file:
    test_packages = [ln.strip() for ln in file.readlines()]

# Definir nuestro paquete
setup(
    name="mineralogia",
    version=0.1,
    description="Clasificación automática de perfiles sinteticos.",
    author="Johann Cambra",
    author_email="johann.cambra@ypf.com",
    python_requires=">=3.9",
    packages=find_namespace_packages(),
    install_requires=[required_packages],
    extras_require={
        "dev": docs_packages
        + quality_packages
        + test_packages
        + security_packages
        + ["pre-commit==2.20.0"],
        "docs": docs_packages,
        "test": test_packages,
    },
)
