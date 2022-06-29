import setuptools

setuptools.setup(
    name="news_dags",
    packages=setuptools.find_packages(exclude=["news_dags_tests"]),
    install_requires=[
        "dagster==0.15.0",
        "dagit==0.15.0",
        "pytest",
    ],
)
