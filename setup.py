from setuptools import setup

setup(
    name = "graphyte",
    version = "0.1.0",
    packages = ["graphyte"],
    author = "Joseph Lee",
    author_email = "joseph@idealist.org",
    description = "Handles requests to Graphite's Render API",
    keywords = ["graphite", "whisper", "carbon", "statsd"],
    url = "http://github.com/josephl/graphyte",
    license = "MIT",
    install_requires = ['numpy', 'pandas', 'requests']
)
