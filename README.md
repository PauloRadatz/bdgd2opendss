# bdgd2opendss

`bdgd2opendss` is a powerful tool designed to facilitate the integration and analysis of distribution systems in OpenDSS. It builds on core functionalities initially developed in the `bdgd-tools` project, extending them with new features focused on improving conversion efficiency and accuracy. In addition to its technical benefits, bdgd2opendss serves an important educational purpose, aimed at democratizing Brazilian distribution system modeling by making real Brazilian distribution system models accessible to students and researchers.

## What does `bdgd2opendss` do?

`bdgd2opendss` is responsible solely for converting models from `bdgd` format to OpenDSS format. It is not intended to verify or validate any data contained in `bdgd`.

## Why a New Repository?

`bdgd2opendss` was created to continue and evolve the work from the `bdgd-tools` project with a clearer focus on performing only the conversion from `bdgd` to OpenDSS. While `bdgd-tools` provided a solid foundation, the decision to create a new repository stems from several key reasons:

1. **Streamlined Focus**: `bdgd2opendss` narrows its focus to exclusively performing the conversion from `bdgd` to OpenDSS, ensuring higher efficiency and precision.
2. **Independence**: The new repository allows for more agile development and greater control over the direction of the project. As the primary maintainer of `bdgd2opendss`, I can ensure that the project aligns with my vision and goals for the future of this work.
3. **Future-Proofing**: Since `bdgd-tools` is unlikely to be maintained or used going forward, `bdgd2opendss` provides a fresh start, ensuring continued development and improvements without being tied to the legacy of the previous project. This new repository will serve as the active and evolving tool for users and collaborators in the future.

By creating `bdgd2opendss`, I aim to push the boundaries of what was accomplished in `bdgd-tools`, while ensuring the project remains relevant, focused, and adaptable to the specific challenges of converting `bdgd` models to OpenDSS.

## Acknowledgments

The development of `bdgd2opendss` builds upon the efforts of the contributors to the original `bdgd-tools` project. Special thanks to the following individuals who played a significant role in the success of the original project:

- **Ênio Rodrigues** - The mastermind behind the outstanding code structure that has made the evolution of the tool easier.
- **Paulo Radatz, Lucas Almeida, and Andrey Lopes** - Responsible for creating the mapping between the `bdgd` and OpenDSS models.
- **Professor Lucas Melo, Miguel Casemiro, and Mozart Nogueira** (Universidade Federal do Ceará) - Improved the tool to near completion and performed valuable validations.
- **Professor Carlos Frederico, Guilherme Broslavschi, Ananda, and Raphael Toshio Sakai** (Universidade de São Paulo) - Contributed to the development of important features, such as the addition of coordinates and energymeters.
- **Ana Camila Mamede** (Universidade Federal de Uberlândia)
- **Rodolfo Londero**

Without their dedication and expertise, this continuation of the project would not have been possible.

## Installation

### Install from PyPI (Recommended)

The easiest way to install `bdgd2opendss` is using pip:

```bash
pip install bdgd2opendss
```

The package supports Python 3.11, 3.12, and 3.13. Dependencies are automatically installed based on your Python version.

### Install from Source

If you prefer to install from source or need to modify the code:

1. Clone the repository:

```bash
git clone https://github.com/pauloradatz/bdgd2opendss.git
```

2. Navigate to the project directory:

```bash
cd bdgd2opendss
```

3. Install the required dependencies:

```bash
pip install -r requirements_py311.txt  # For Python 3.11
pip install -r requirements_py312.txt  # For Python 3.12
pip install -r requirements_py313.txt  # For Python 3.13
```

4. Install the package in editable mode:

```bash
pip install -e .
```

### Installation Support

If you need support on installation and usage, or if you're interested in learning everything about creating virtual environments, setting up Python projects using PyCharm, and managing Python projects necessary for installation and use, I strongly recommend my Python course [Python Sem Mistérios: Instalação, Ambientes Virtuais e Gerenciamento de Projetos](https://paulo-radatz.kpages.online/pagina-de-vendas-39e03b9e-c528-4d0c-b831-f5593ef0baa7). Aula 6.1 summarizes the process of downloading and installing a repository like this one.

## Usage

After installation, you can start using `bdgd2opendss` by importing it into your Python scripts or running the provided command-line tools. Detailed usage examples can be found in the `examples` folder or in the documentation.

```python
import bdgd2opendss
# Example usage
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for more information.

## Contributing

Contributions to `bdgd2opendss` are welcome! Please refer to our contribution guidelines in `CONTRIBUTING.md` for details on how you can help improve the project.

## How to Cite

If you use `bdgd2opendss` in your academic work, please reference it as follows:

**APA Style:**

    Radatz, P., & Contributors. (2024). bdgd2opendss: A BDGD to OpenDSS conversion tool (Version X.X.X) [Computer software]. GitHub. https://github.com/pauloradatz/bdgd2opendss

**BibTeX Entry:**

```bibtex
@software{radatz2024bdgd2opendss,
  author = {Paulo Radatz and Contributors},
  title = {bdgd2opendss: A BDGD to OpenDSS conversion tool},
  year = {2024},
  version = {X.X.X},
  url = {https://github.com/pauloradatz/bdgd2opendss}
}
```

Please replace `X.X.X` with the version of the package you are using.

## Contact

For questions, support, or consulting inquiries, please contact Paulo Radatz at [paulo.radatz@gmail.com](mailto:paulo.radatz@gmail.com).

