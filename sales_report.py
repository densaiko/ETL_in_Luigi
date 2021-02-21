import luigi
from luigi import LocalTarget, Task


class ProcessOrder(Task):
    def output(self):
        return LocalTarget('orders.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May,100', file=f)
            print('May,180', file=f)
            print('June,200', file=f)
            print('June,210', file=f)


class GenerateReport(Task):
    def requires(self):
        return ProcessOrder()

    def output(self):
        return LocalTarget('report.csv')

    def run(self):
        report = {}
        for line in self.input().open():
            month, amount = line.split(',')
            if month in report:
                report[month] += float(amount)
            else:
                report[month] = float(amount)

        with self.output().open('w') as out:
            for month in report:
                print(month + ',' + str(report[month]), file=out)


if __name__ == '__main__':
    luigi.run(['GenerateReport', '--local-scheduler'])
