from setuptools import find_packages, setup


if __name__ == "__main__":
    setup(
        name="bitrix-taxi-router",
        version="0.1.0",
        description="Bitrix24 lead distribution with timed reassignment",
        package_dir={"": "src"},
        packages=find_packages("src"),
        install_requires=["fastapi", "uvicorn"],
    )
