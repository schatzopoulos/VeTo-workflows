"""PaperVeto Class"""
from pathlib import Path
import csv

from paper_recommender.paper_veto import AbstractVeto


class ExtendedPaperVeto(AbstractVeto):
    """Wrapper class for running extended veto"""

    def __init__(self, paper_file, veto_output, pap_sims, ptp_sims, sims_per_paper=50,
                 pap_weight=0.5, ptp_weight=0.5, algorithm='borda', rrf_k=0, output_size=20,
                 keyword_weight=0.5):
        super().__init__(paper_file, veto_output, pap_sims, ptp_sims, sims_per_paper, pap_weight,
                         ptp_weight, algorithm, rrf_k, output_size)
        self.keyword_weight = keyword_weight

    def __str__(self):
        return f'ExtendedPaperVeto({id(self)})'

    def run(self):
        """The Run algorithm"""
        train_set = {}
        with open(self.paper_file) as train_file:
            train_entries = csv.reader(train_file, dialect='exp_dialect')
            for entry in train_entries:
                train_set[entry[0]] = 1

        # Get suggestions based on HIN
        hin_sugg = {}
        for entry in train_set:
            try:
                with open(Path(self.pap_sims, entry)) as pap_paper_file:
                    pap_papers = csv.reader(pap_paper_file, dialect='exp_dialect')
                    lines_to_read = self.sims_per_paper
                    for paper in pap_papers:
                        if paper[1] in train_set.keys():  # do not consider anyone in the training set
                            continue
                        lines_to_read -= 1
                        if lines_to_read == -1:
                            break
                        if paper[1] in hin_sugg.keys():
                            hin_sugg[paper[1]]['pap'] += self.score(self.pap_weight, lines_to_read, paper[2])
                        else:
                            hin_sugg[paper[1]] = {}
                            hin_sugg[paper[1]]['ptp'] = 0
                            hin_sugg[paper[1]]['keyword'] = 0
                            hin_sugg[paper[1]]['pap'] = self.score(self.pap_weight, lines_to_read, paper[2])
            except FileNotFoundError:
                pass

            try:
                with open(Path(self.ptp_sims, entry)) as ptp_paper_file:
                    ptp_papers = csv.reader(ptp_paper_file, dialect='exp_dialect')
                    lines_to_read = self.sims_per_paper
                    for paper in ptp_papers:
                        if paper[1] in train_set.keys():  # do not consider anyone in the training set
                            continue
                        lines_to_read -= 1
                        if lines_to_read == -1:
                            break
                        if paper[1] in hin_sugg.keys():
                            hin_sugg[paper[1]]['ptp'] += self.score(self.ptp_weight, lines_to_read, paper[2])
                        else:
                            hin_sugg[paper[1]] = {}
                            hin_sugg[paper[1]]['ptp'] = self.score(self.ptp_weight, lines_to_read, paper[2])
                            hin_sugg[paper[1]]['pap'] = 0
                            hin_sugg[paper[1]]['keyword'] = 0
            except FileNotFoundError:
                pass

        for sugg in hin_sugg.keys():
            hin_sugg[sugg]['overall'] = hin_sugg[sugg]['ptp'] + hin_sugg[sugg]['pap'] + hin_sugg[sugg]['keyword']

        hin_sugg_list = sorted(hin_sugg, key=lambda k: hin_sugg[k]['overall'],
                               reverse=True)  # sort suggestions based on borda count
        hin_sugg_list = hin_sugg_list[0:self.output_size]  # keep as many as in the test size

        with open(self.veto_output, 'w', newline='') as hin_sugg_file:
            hin_sugg_writer = csv.writer(hin_sugg_file)
            for sugg in hin_sugg_list:
                hin_sugg_writer.writerow([sugg,
                                          round(hin_sugg[sugg]['overall'], 2),
                                          round(hin_sugg[sugg]['ptp'], 2),
                                          round(hin_sugg[sugg]['pap'], 2),
                                          round(hin_sugg[sugg]['keyword'], 2)
                                          ])


if __name__ == '__main__':
    enhanced_veto = ExtendedPaperVeto.create_from_args()
    enhanced_veto.run()
