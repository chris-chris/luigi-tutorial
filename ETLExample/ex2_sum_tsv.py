#-*- coding:utf-8 -*-
import luigi
import csv
import os
import os.path


from datetime import date

import ex1_read_csv

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

#PYTHONPATH='' luigi --module ex2_sum_tsv Ex2SumTSV --local-scheduler

class Ex2SumTSV(luigi.Task):

  date = luigi.DateParameter(default=date.today())
  custom = luigi.Parameter(default="")

  def requires(self):
    yield ex1_read_csv.Ex1ReadCSV(date=self.date)

  def run(self):
    filepath = CURRENT_DIR + "/data_%s.tsv" % self.date
    print(filepath)
    with open(filepath) as csvfile:
      spamreader = csv.reader(csvfile, delimiter='\t', quotechar='|')
      for row in spamreader:
        print('\t'.join(row))


if __name__ == "__main__":
  task = Ex2SumTSV()
  task.run()
