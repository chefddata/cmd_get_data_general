from setuptools import setup

setup(
    name = 'get_itemCountPerCustomer',
    version = '0.1.0',
    packages = ['get_itemCountPerCustomer'],
    entry_points = {
        'console_scripts': [
            'get_itemCountPerCustomer = get_itemCountPerCustomer.__main__:main'
        ]
    })
