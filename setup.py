import setuptools

setuptools.setup(
    name="nuforc",
    version="0.1",
    description="Package containing custom Airflow components for NUFORC project.",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=["apache-airflow~=2.0.0b3"],
    python_requires=">=3.7.*",
)
