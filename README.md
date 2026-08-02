# bdgd2opendss

O `bdgd2opendss` é uma ferramenta criada para facilitar a conversão e análise de sistemas de distribuição de energia no OpenDSS. Ele foi desenvolvido a partir do projeto `bdgd-tools`, trazendo novos recursos para deixar a conversão mais rápida, precisa e simples. Além dos benefícios técnicos, a ferramenta tem um papel educacional importante: democratizar a modelagem de redes elétricas no Brasil, tornando modelos reais acessíveis para estudantes, pesquisadores e engenheiros.

## O que o `bdgd2opendss` faz (e o que NÃO faz)

### O que ele faz:
- **Suporta BDGD pública e privada**: Converte tanto as bases públicas disponibilizadas nos Dados Abertos da ANEEL quanto as bases privadas internas das distribuidoras.
- **Modelagem alinhada com a ANEEL**: Replicamos a metodologia de conversão adotada pela ANEEL no cálculo de perdas técnicas (validada e publicada em [artigo acadêmico](https://www.sba.org.br/open_journal_systems/index.php/sbai/article/view/5172)).
- **Simplicidade**: Realiza a conversão completa em poucas linhas de código em Python.

### O que ele NÃO faz:
- **Não corrige dados da BDGD**: A ferramenta é exclusivamente um conversor. Se a BDGD de origem contiver dados incorretos ou incompletos, o modelo OpenDSS gerado refletirá essas inconsistências.
- **Não entrega o modelo calibrado para qualquer estudo**: O modelo gerado é a representação física direta da BDGD. Para estudos específicos (como fluxo de potência, análise de tensão ou fluxo reverso), o usuário deve fazer os devidos ajustes de demanda e energia conforme o objetivo da análise.

### Limitações e considerações da modelagem:
- **Elementos ANEEL**: Converte apenas os elementos considerados pela ANEEL no cálculo de perdas técnicas. Alguns exemplos incluem: capacitores não serem criados no OpenDSS, a utilização de dois modelos de carga para os consumidores e a modelagem do neutro conforme o padrão regulatório.
- **Rede Fixa**: O modelo utiliza uma rede estática para o ano todo, sem considerar alterações operacionais ao longo do tempo (como transferências de carga entre alimentadores). Isso pode fazer com que a energia medida no início do alimentador seja muito diferente da energia calculada no modelo.
- **Geração Distribuída (GD)**: A ferramenta de perdas da ANEEL não cria elementos de GD no OpenDSS. O `bdgd2opendss` gera os arquivos DSS com GDs, mas apenas a localização (barra/bus) é totalmente confiável. Os demais parâmetros necessitam de estudos mais detalhados ou de uma padronização futura da ANEEL. Além disso:
  - **Consumidor com consumo líquido**: É modelado aplicando a energia consumida líquida sobre a curva de carga típica. A GD é considerada no balanço de energia, mas não reflete o perfil instantâneo real de consumo, o que pode limitar alguns estudos.
  - **Consumidor com geração líquida**: A energia gerada líquida não é considerada no modelo matemático. Isso limita estudos como o de fluxo reverso e pode fazer com que a energia medida no início do alimentador seja menor que a energia calculada no modelo.

## Por que um novo repositório?

O `bdgd2opendss` foi criado para dar continuidade ao `bdgd-tools`, mas com foco exclusivo em fazer a conversão da BDGD para o OpenDSS. O `bdgd-tools` deu uma base muito boa, mas decidi criar este novo repositório por alguns motivos principais:

1. **Foco**: O `bdgd2opendss` faz apenas a conversão da BDGD para o OpenDSS, o que garante mais eficiência e precisão.
2. **Independência**: Um novo repositório permite desenvolver mais rápido e ter mais controle sobre o futuro do projeto. Como principal mantenedor, consigo garantir que a ferramenta siga alinhada com o que planejo para ela.
3. **Futuro do projeto**: O `bdgd-tools` não deve mais receber atualizações. O `bdgd2opendss` é um recomeço para continuar evoluindo sem ficar preso ao código antigo.

## Agradecimentos

O `bdgd2opendss` existe graças ao trabalho do pessoal que contribuiu no projeto original `bdgd-tools`. Um agradecimento especial para:

- **Ênio Rodrigues** - Criou a estrutura de código inicial que facilitou muito a evolução da ferramenta.
- **Paulo Radatz, Lucas Almeida e Andrey Lopes** - Criaram o mapeamento entre o BDGD e o OpenDSS.
- **Professor Lucas Melo, Miguel Casemiro e Mozart Nogueira** (Universidade Federal do Ceará) - Melhoraram a ferramenta e fizeram validações importantes.
- **Professor Carlos Frederico, Guilherme Broslavschi, Ananda e Raphael Toshio Sakai** (Universidade de São Paulo) - Ajudaram com recursos importantes, como coordenadas e medidores de energia (*energymeters*).
- **Ana Camila Mamede** (Universidade Federal de Uberlândia)
- **Rodolfo Londero**

Sem a ajuda e a dedicação de vocês, este projeto não seria possível.

## Instalação

### Instalar via PyPI (Recomendado)

A forma mais fácil de instalar o `bdgd2opendss` é usando o pip:

```bash
pip install bdgd2opendss
```

O pacote roda no Python 3.11, 3.12, 3.13 e 3.14. As dependências são instaladas automaticamente de acordo com a sua versão do Python.

### Instalar pelo código-fonte

Se você quiser modificar o código ou instalar direto do repositório:

1. Clone o repositório:

```bash
git clone https://github.com/pauloradatz/bdgd2opendss.git
```

2. Entre na pasta do projeto:

```bash
cd bdgd2opendss
```

3. Instale as dependências:

```bash
pip install -r requirements_py311.txt  # Para Python 3.11
pip install -r requirements_py312.txt  # Para Python 3.12
pip install -r requirements_py313.txt  # Para Python 3.13
pip install -r requirements_py314.txt  # Para Python 3.14
```

4. Instale o pacote em modo editável:

```bash
pip install -e .
```

## Como usar

Depois de instalar, você pode converter uma BDGD para OpenDSS com poucas linhas de código em Python:

```python
import bdgd2opendss

# Caminho da sua BDGD (.gdb) e pasta onde deseja salvar os arquivos OpenDSS
bdgd_path = "caminho/para/sua_bdgd.gdb"
output_folder = "caminho/para/pasta_de_saida"

# (Opcional) Listar os alimentadores disponíveis na BDGD
feeders = bdgd2opendss.get_feeder_list(bdgd_path)
print("Alimentadores encontrados:", feeders)

# Executar a conversão completa
bdgd2opendss.run(bdgd_path, output_folder)
```

## Cursos e Treinamentos

Se você quiser aprender a usar o `bdgd2opendss` e se aprofundar no OpenDSS, confira os cursos disponíveis na [Radatz Academy BR](https://www.pauloradatz.me/radatz-academy-br).

## Licença

Este projeto usa a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## Contribuições

Quer ajudar a melhorar o `bdgd2opendss`? Toda contribuição é bem-vinda! Veja como ajudar no arquivo `CONTRIBUTING.md`.

## Como citar

Para mais detalhes sobre a validação da ferramenta, você pode ler o [artigo publicado no SBAI](https://www.sba.org.br/open_journal_systems/index.php/sbai/article/view/5172).

Se você usar o `bdgd2opendss` no seu trabalho acadêmico, pode citar assim:

**Estilo APA:**

    Radatz, P., & Contributors. (2026). bdgd2opendss: A BDGD to OpenDSS conversion tool (Version 1.2.5) [Computer software]. GitHub. https://github.com/pauloradatz/bdgd2opendss

**BibTeX:**

```bibtex
@software{radatz2024bdgd2opendss,
  author = {Paulo Radatz and Contributors},
  title = {bdgd2opendss: A BDGD to OpenDSS conversion tool},
  year = {2026},
  version = {1.2.5},
  url = {https://github.com/pauloradatz/bdgd2opendss}
}
```

Se estiver usando outra versão, é só mudar o número da versão na citação.

## Contato

Se tiver dúvidas, precisar de suporte ou quiser contratar consultoria, pode me mandar um e-mail: [paulo.radatz@gmail.com](mailto:paulo.radatz@gmail.com).
