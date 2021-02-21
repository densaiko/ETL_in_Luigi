import luigi
from luigi import Task, LocalTarget
from luigi.contrib import sqla
from sqlalchemy import String, Float


class DownloadFranceSales(Task):
    def output(self):
        return LocalTarget('France.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May,100', file=f)
            print('June,2000', file=f)


class DownloadGermanySales(Task):
    def output(self):
        return LocalTarget('Germany.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May,2000', file=f)
            print('June,10', file=f)


class CreateDatabase(sqla.CopyToTable):
    columns = [
        (['month', String(64)], {}),
        (['amount', Float], {})
    ]
    connection_string = "sqlite:///test.db"
    table = "sales"
    column_separator = ','

    def rows(self):
        with self.input()[0].open() as f:
            for line in f:
                yield line.split(self.column_separator)
        with self.input()[1].open() as f:
            for line in f:
                yield line.split(self.column_separator)

    def requires(self):
        return [DownloadFranceSales(),
                DownloadGermanySales()]


if __name__ == '__main__':
    luigi.run(['CreateDatabase', '--local-scheduler'])