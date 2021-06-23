import pandas as pd
pd.options.mode.chained_assignment = None

FILE_NAME = "./Datos CSV/Mental Health Data Sin CID.csv"
FILE_NAME_CID = "./Datos CSV/Mental Health Data.csv"
DEST_FILE_NAME = "./Datos CSV/Datos Mental Health - Practice Size {}.csv"
FASTA_FILE = "./FASTA/MH PSC = {}.fas"

CLIENT_ID_COLUMN = "CustomerID"
NEW_ID_COLUMN_NAME = "CID_Outside_Identifier"

EVALUATOR = "evaluator"
HIGH_AMINO = "high"
MID_AMINO = "mid"
LOW_AMINO = "low"
sequence_ids = {HIGH_AMINO: "A", MID_AMINO: "M", LOW_AMINO: "B"}
AMINO_ACID_DATA_MAPPER = {
    'CFPR': {
        # MID = (0.98 * 75)/100 = 0.735
        EVALUATOR: lambda x: HIGH_AMINO if x >= 0.98 else (MID_AMINO if 0.98 > x >= 0.735 else LOW_AMINO),
        HIGH_AMINO: "WRWRWRWRWR", MID_AMINO: "RNRNRNRNRN", LOW_AMINO: "AAAAAAAAAA"
    },
    'NCR': {
        # MID = (0.96 * 75)/100 = 0.72
        EVALUATOR: lambda x: HIGH_AMINO if x >= 0.96 else (MID_AMINO if 0.96 > x >= 0.72 else LOW_AMINO),
        HIGH_AMINO: "WQWQWQWQWQ", MID_AMINO: "DQDQDQDQDQ", LOW_AMINO: "ALALALALAL"
    },
    'CPL45': {
        # MID = (0.9 * 75)/100 = 0.675
        EVALUATOR: lambda x: HIGH_AMINO if x >= 0.90 else (MID_AMINO if 0.9 > x >= 0.675 else LOW_AMINO),
        HIGH_AMINO: "WKWKWKWKWK", MID_AMINO: "GEGEGEGEGE", LOW_AMINO: "AIAIAIAIAI"
    },
    'DARO120': {
        # MID = 0.10 + ((0.10 * 25)/100) = 0.125
        EVALUATOR: lambda x: HIGH_AMINO if x <= 0.10 else (MID_AMINO if 0.10 < x <= 0.125 else LOW_AMINO),
        HIGH_AMINO: "WMWMWMWMWM", MID_AMINO: "MFMFMFMFMF", LOW_AMINO: "ASASASASAS"
    },
    'DAR': {
        # MID = 40 + ((40 * 25)/100) = 50
        EVALUATOR: lambda x: HIGH_AMINO if x <= 40 else (MID_AMINO if 40 < x <= 50 else LOW_AMINO),
        HIGH_AMINO: "WTWTWTWTWT", MID_AMINO: "SPSPSPSPSP", LOW_AMINO: "AVAVAVAVAV"
    },
}


def create_client_id_key(dataset: pd.DataFrame):
    keys_dict = {}
    unique_id = 0

    for key in dataset[CLIENT_ID_COLUMN]:
        try:
            keys_dict[key]
        except KeyError:
            keys_dict[key] = str(unique_id)
            unique_id += 1
    dataset[NEW_ID_COLUMN_NAME] = dataset[CLIENT_ID_COLUMN]
    dataset[NEW_ID_COLUMN_NAME] = dataset[NEW_ID_COLUMN_NAME].map(keys_dict)
    dataset[NEW_ID_COLUMN_NAME] = dataset[NEW_ID_COLUMN_NAME] + "-" + \
                                  dataset["Practice Size Code"].astype(str) + "-" + \
                                  dataset["Specialty Code"].astype(str)
    return dataset


def convert_to_amino_acid_values(dataset: pd.DataFrame):
    dataset["sequence"] = ""
    dataset["metrics_status_id"] = ""

    for key, values_dict in AMINO_ACID_DATA_MAPPER.items():
        new_column_key = key + "-Amino"
        new_column_key_stat = key + "-Status"

        dataset[new_column_key] = dataset[key]
        dataset[new_column_key_stat] = dataset[key]

        value_mapper = {}
        value_mapper_stat = {}
        for value in dataset[key].unique():
            eval_res = values_dict[EVALUATOR](value)
            value_mapper[value] = values_dict[eval_res]
            value_mapper_stat[value] = sequence_ids[eval_res]
        dataset[new_column_key] = dataset[new_column_key].map(value_mapper)
        dataset[new_column_key_stat] = dataset[new_column_key_stat].map(value_mapper_stat)
        dataset["sequence"] = dataset["sequence"] + dataset[new_column_key]
        dataset["metrics_status_id"] = dataset["metrics_status_id"] + dataset[new_column_key_stat]
        dataset = dataset.drop(new_column_key_stat, axis=1)
    dataset["CID_Outside_Identifier"] = dataset["CID_Outside_Identifier"] + "-" + dataset["metrics_status_id"]
    return dataset


def create_fasta_file(file_name, df):
    with open(file_name, 'w+') as file:
        for row in df.iterrows():
            file.write(">{}\n{}\n".format(row[1][0], row[1][1]))


if __name__ == '__main__':
    df = pd.read_csv(FILE_NAME)
    df = create_client_id_key(df)
    df.to_csv(FILE_NAME_CID, index=False)

    for i in range(5):
        psc = df.loc[df["Practice Size Code"] == (i + 1)]
        df_amino = convert_to_amino_acid_values(psc)
        create_fasta_file(FASTA_FILE.format(i + 1), df_amino[["CID_Outside_Identifier", "sequence"]])
        df_amino.to_csv(DEST_FILE_NAME.format(i + 1), index=False)
