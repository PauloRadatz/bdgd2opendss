import csv
import os.path
import pandas as pd
import os
import pathlib
import py_dss_tools


'''
modificar os endereços dos arquivos das redes a serem comparadas

'''



def log_errors_elements2(df_aneel, df_tools,arquivo="log_errors"):

    print(os.getcwd())
    if not os.path.exists("logs_errors"):
        os.mkdir("logs_errors")

    output_directory = os.path.join('logs_errors')
    path = os.path.join(output_directory, f'{arquivo}.csv')



    df_aneel = df_aneel.sort_values(by='name')
    df_aneel = df_aneel.reset_index(drop=True)

    df_tools = df_tools.sort_values(by='name')
    df_tools = df_tools.reset_index(drop=True)

    merged_df = pd.merge(df_aneel, df_tools, on='name', suffixes=('_df1', '_df2'), how='inner')

    # Abre um arquivo CSV para escrita no caminho especificado
    with open(path, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Escreve o cabeçalho no arquivo CSV
        writer.writerow(['Coluna', 'Nome', 'Valor Aneel', 'Valor Df'])

        for col_A in df_aneel.columns:
            if col_A == 'name':
                continue

            comparison_result = merged_df[f'{col_A}_df1'] == merged_df[f'{col_A}_df2']
            false_elements = comparison_result[not comparison_result]

            for index, value in false_elements.items():
                # Adiciona as informações ao arquivo CSV
                writer.writerow([col_A, merged_df.loc[index, 'name'], merged_df.loc[index, f'{col_A}_df1'], merged_df.loc[index, f'{col_A}_df2']])

    print(f'Arquivo {arquivo} gerado')


def remove_middle_number(name):
    parts = name.split('_')
    if len(parts) >= 4:  # Ensure there's a numeric section to remove
        parts.pop(2)  # Remove the middle numeric section
    return '_'.join(parts)


# Get the current working directory
cwd = os.getcwd()

print(cwd)
dss_file = r'C:\\Users\\ADERALDO\\Desktop\\TCC BDGB\\BDGD_Dev\\bdgd-tools\\output\\1_3PAS_1\\Master_DO_01_1_3PAS_1.dss'
study = py_dss_tools.CreateStudy.generic(name="Test", dss_file=dss_file)
df_loads = study.model.loads_df
df_s = study.model.summary_df
df_lines = study.model.lines_df
df_linecodes = study.model.linecodes_df

df_trafos = study.model.transformers_df
df_loadshape = study.model.loadshapes_df
study = py_dss_tools.CreateStudy.power_flow(name="Test", dss_file=dss_file)
study.dss.text("solve")
study.view.voltage_profile()
resultado_nos = study.results.voltage_ln_nodes[0]

dss_file_aneel = r'c:\\Users\\ADERALDO\\Desktop\\TCC BDGB\\py-dss-tools\\examples\\feeders\\3PAS\\Master_DO01_202212598_1_3PAS_1_--MBSR1-----.dss'
study_aneel = py_dss_tools.CreateStudy.generic(name="TestAneel", dss_file=dss_file_aneel)

df_aneel_loads = study_aneel.model.loads_df

df_aneel_lines = study_aneel.model.lines_df
df_aneel_linecodes = study_aneel.model.linecodes_df
df_aneel_trafos = study_aneel.model.transformers_df
df_aneel_loadshapes = study_aneel.model.loadshapes_df
df_s_aneel = study_aneel.model.summary_df
resultado_nos_aneel = study_aneel.results.voltage_ln_nodes[0]

study_aneel = py_dss_tools.CreateStudy.power_flow(
    name="Test", dss_file=dss_file_aneel
)
study.dss.text("solve")
study_aneel.view.voltage_profile()


for entidade in ['sbt', 'smt', 'rbt', 'cmt']:
    try:
        df_aneel_entidade = df_aneel_lines[df_aneel_lines['name'].str.contains(entidade)]
        df_entidade = df_lines[df_lines['name'].str.contains(entidade)]
        log_errors_elements2(df_aneel_entidade, df_entidade, arquivo=f'{entidade}_logs_errors')
    except KeyError:
        print("there is no {entidade} element found")# Handle the KeyError exception here if needed
log_errors_elements2(df_aneel_trafos, df_trafos, arquivo='trafos_logs_errors')

log_errors_elements2(
    df_aneel_loadshapes, df_loadshape, arquivo='loadshape_logs_errors'
)

log_errors_elements2(
    df_aneel_linecodes, df_linecodes, arquivo='linecodes_logs_errors'
)

df_loads['name'] = df_loads['name'].apply(remove_middle_number)


log_errors_elements2(df_aneel_loads, df_loads, arquivo='loads_logs_errors')
