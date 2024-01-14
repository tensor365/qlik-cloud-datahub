from setuptools import find_packages, setup

setup_output = setup(
    name="QlikCloudSource",
    version="0.1",
    description="Connector to Qlik Cloud Source for Data Hub",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["acryl-datahub", "qlik-sdk"],
)