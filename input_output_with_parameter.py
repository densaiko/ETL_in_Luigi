import os

import luigi
from luigi import Task, Parameter, LocalTarget

INPUT_FOLDER = 'input'
OUTPUT_FOLDER = 'output'


class DownloadFile(Task):
    file_name = Parameter()

    def output(self):
        path = os.path.join(OUTPUT_FOLDER, self.file_name)
        return LocalTarget(path)

    def run(self):
        input_path = os.path.join(INPUT_FOLDER, self.file_name)
        with open(input_path) as f:
            with self.output().open('w') as out:
                for line in f:
                    if ',' in line:
                        out.write(line)


class DownloadSalesData(Task):
    def output(self):
        return LocalTarget('all_sales.csv')

    def run(self):
        processed_files = []
        for file in os.listdir(INPUT_FOLDER):
            target = yield DownloadFile(file)
            processed_files.append(target)

        with self.output().open('w') as out:
            for file in processed_files:
                with file.open() as f:
                    for line in f:
                        out.write(line)


if __name__ == '__main__':
    luigi.run(['DownloadSalesData'])
