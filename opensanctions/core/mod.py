import csv
import sys

gendate_dict = {}

def insert_gendate(dataset_name, gendate):
    gendate_dict[dataset_name] = gendate

def export_gendate(file_path):
    try:
        with open(file_path, 'w') as f:
            for key, value in zip(gendate_dict.keys(), gendate_dict.values()):
                f.write("{},{}\n".format(key, value))
    except IOError:
        print("I/O error: MOD: export: failed saving date to csv file", file=sys.stderr)
    except Exception:
        print("Error: MOD: export: exception", file=sys.stderr)
        