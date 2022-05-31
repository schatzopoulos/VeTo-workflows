"""PaperVeto Class"""
from pathlib import Path
import csv

from paper_recommender.paper_veto import VetoBase


class ExtendedPaperVeto(VetoBase):
    """Wrapper class for running extended veto"""

    def __init__(self, paper_file, veto_output, pap_sims, ptp_sims, sims_per_paper=50,
                 pap_weight=0.5, ptp_weight=0.5, algorithm='borda', rrf_k=0, output_size=20,
                 keyword_weight=0.5):
        super().__init__(paper_file, veto_output, pap_sims, ptp_sims, sims_per_paper, pap_weight,
                         ptp_weight, algorithm, rrf_k, output_size)
        self.keyword_weight = keyword_weight

    def __str__(self):
        return f'ExtendedPaperVeto({id(self)})'

    def _calculate_sim_scores(self, entry, train_set, first_key, second_key, third_key, weight, output):
        """Calculate Similarity scores"""
        try:
            with open(Path(self.pap_sims, entry)) as pap_paper_file:
                pap_papers = csv.reader(pap_paper_file, dialect='exp_dialect')
                lines_to_read = self.sims_per_paper
                for paper in pap_papers:
                    if paper[1] in train_set.keys():  # do not consider anyone in the training set
                        continue
                    if paper[1] in output.keys():
                        output[paper[1]][first_key] += self.score(weight, lines_to_read, paper[2])
                    else:
                        output[paper[1]] = {}
                        output[paper[1]][first_key] = self.score(weight, lines_to_read, paper[2])
                        output[paper[1]][second_key] = 0
                        output[paper[1]][third_key] = 0
                    lines_to_read -= 1
                    if lines_to_read == 0:
                        break
        except FileNotFoundError:
            pass

    def _write_results(self, output):
        """Write the results"""
        for sugg in output.keys():
            output[sugg]['overall'] = output[sugg]['ptp'] + output[sugg]['pap'] + output[sugg]['keyword']

        scoring_list = self._get_scoring_list(output)
        with open(self.veto_output, 'w', newline='') as hin_sugg_file:
            hin_sugg_writer = csv.writer(hin_sugg_file, delimiter=',')
            for sugg in scoring_list:
                hin_sugg_writer.writerow([sugg,
                                          round(output[sugg]['overall'], 2),
                                          round(output[sugg]['ptp'], 2),
                                          round(output[sugg]['pap'], 2),
                                          round(output[sugg]['keyword'], 2)
                                          ])

    def run(self):
        """The Run algorithm"""
        train_set = self._get_train_set()
        hin_sugg = {}
        for entry in train_set:
            self._calculate_sim_scores(entry, train_set, 'pap', 'ptp', 'keyword', self.pap_weight, hin_sugg)
            self._calculate_sim_scores(entry, train_set, 'ptp', 'pap', 'keyword', self.ptp_weight, hin_sugg)
        self._write_results(hin_sugg)


if __name__ == '__main__':
    enhanced_veto = ExtendedPaperVeto.create_from_args()
    enhanced_veto.run()
