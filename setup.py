from setuptools import setup, find_packages


install_requires = [
    'requests',
    'pandas'
]

setup(
    name='lesson_creator',
    version="0.0.0.dev1",
    description='Lyceum lesson creator on',
    platforms=['POSIX'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'create_lessons=lesson_creator.main:create_all',
        ]
    }
)
