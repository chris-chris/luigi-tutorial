#-*- coding:utf-8 -*-
import luigi
import csv
import os
import os.path


from datetime import date

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

#PYTHONPATH='' luigi --module ex1_read_csv Ex1ReadCSV --local-scheduler

class Ex1ReadCSV(luigi.Task):

  date = luigi.DateParameter(default=date.today())
  custom = luigi.Parameter(default="")

  def run(self):
    filepath = CURRENT_DIR + "/data.csv"
    filepath_out = CURRENT_DIR + "/data_%s.tsv" % self.date
    print(filepath)
    with open(filepath_out, "wb") as out:
      with open(filepath) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in spamreader:
          print('\t'.join(row))
          out.write('\t'.join(row)+"\n")

  def complete(self):

    if(self.custom == "force"):
      return False

    filepath_out = CURRENT_DIR + "/data_%s.tsv" % self.date
    print(filepath_out)
    if(os.path.exists(filepath_out)):
      print("exists")
      return True
    else:
      print("not exists")

      return False

if __name__ == "__main__":
  task = Ex1ReadCSV()
  task.run()
