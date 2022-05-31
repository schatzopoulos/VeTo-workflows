"""PaperVeto Class"""
import argparse
from pathlib import Path
import csv

# Define CSV dialect to be used.
csv.register_dialect(
    'exp_dialect',
    delimiter='\t'
)


class VetoBase:
    """Veto base class"""

    def __init__(self, paper_file, veto_output, pap_sims, ptp_sims, sims_per_paper=50,
                 pap_weight=0.5, ptp_weight=0.5, algorithm='borda', rrf_k=0, output_size=20):
        self.paper_file = paper_file
        self.veto_output = veto_output
        self.pap_sims = pap_sims
        self.ptp_sims = ptp_sims
        self.sims_per_paper = sims_per_paper
        self.pap_weight = pap_weight
        self.ptp_weight = ptp_weight
        self.algorithm = algorithm
        self.rrf_k = rrf_k
        self.output_size = output_size

    def __str__(self):
        return f'BaseVetoVeto({id(self)})'

    @classmethod
    def create_from_args(cls):
        """Create from user arguments"""
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('-pf', '--paper_file', required=True, help='paper ids file')
        arg_parser.add_argument('-vo', '--veto_output', required=True, help='file where the results will be written')
        arg_parser.add_argument('-pap', '--pap_sims', required=True, help='directory containing the PAP similarities')
        arg_parser.add_argument('-ptp', '--ptp_sims', required=True, help='directory containing the PTP similarities')
        arg_parser.add_argument('-spe', '--sims_per_paper', nargs='?', type=int, default=50,
                                help='how many similarities per paper should be considered')
        arg_parser.add_argument('-papw', '--pap_weight', nargs='?', type=float, default=0.5,
                                help='score weight for the PAP similarities')
        arg_parser.add_argument('-ptpw', '--ptp_weight', nargs='?', type=float, default=0.5,
                                help='score weight for the PTP similarities')
        arg_parser.add_argument('-algo', '--algorithm', choices=['borda', 'rrf', 'sum'], default='borda',
                                help='the scoring algorithm to be used')
        arg_parser.add_argument('-rrfk', '--rrf_k', nargs='?', type=int, default=0,
                                help='rrf k algorithm ranking parameter')
        arg_parser.add_argument('-outs', '--output_size', nargs='?', type=int, default=20,
                                help='the size of the output')
        veto_args = arg_parser.parse_args()

        return cls(
            paper_file=veto_args.paper_file,
            veto_output=veto_args.veto_output,
            pap_sims=veto_args.pap_sims,
            ptp_sims=veto_args.ptp_sims,
            sims_per_paper=veto_args.sims_per_paper,
            pap_weight=veto_args.pap_weight,
            ptp_weight=veto_args.ptp_weight,
            algorithm=veto_args.algorithm,
            rrf_k=veto_args.rrf_k,
            output_size=veto_args.output_size
        )

    def score(self, coeff, lines_to_read, sim_score):
        """Scoring method"""
        if self.algorithm == 'borda':
            return coeff * lines_to_read
        elif self.algorithm == 'rrf':
            return coeff * (1.0 / (self.rrf_k + (self.sims_per_paper - lines_to_read)))
        elif self.algorithm == 'sum':
            return float(sim_score)

    def _get_train_set(self):
        """Get the train set"""
        train_set = {}
        with open(self.paper_file) as train_file:
            train_entries = csv.reader(train_file, dialect='exp_dialect')
            for entry in train_entries:
                train_set[entry[0]] = 1
        return train_set

    def _calculate_sim_scores(self, *args, **kwargs):
        raise NotImplementedError()

    def _get_scoring_list(self, output):
        """Gets the scoring list"""
        scoring_list = sorted(output, key=lambda k: output[k]['overall'], reverse=True)
        return scoring_list[0:self.output_size]  # keep as many as in the test size

    def _write_results(self, *args, **kwargs):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()


class PaperVeto(VetoBase):
    """BaseVeto class for veto"""

    def _calculate_sim_scores(self, entry, train_set, first_key, second_key, weight, output):
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
                    lines_to_read -= 1
                    if lines_to_read == 0:
                        break
        except FileNotFoundError:
            pass

    def _write_results(self, output):
        """Write the results"""
        for sugg in output.keys():
            output[sugg]['overall'] = output[sugg]['ptp'] + output[sugg]['pap']

        scoring_list = self._get_scoring_list(output)
        with open(self.veto_output, 'w', newline='') as hin_sugg_file:
            hin_sugg_writer = csv.writer(hin_sugg_file, delimiter=',')
            for sugg in scoring_list:
                hin_sugg_writer.writerow([sugg,
                                          round(output[sugg]['overall'], 2),
                                          round(output[sugg]['ptp'], 2),
                                          round(output[sugg]['pap'], 2)
                                          ])

    def run(self):
        """The Run algorithm"""
        train_set = self._get_train_set()
        hin_sugg = {}
        for entry in train_set:
            self._calculate_sim_scores(entry, train_set, 'pap', 'ptp', self.pap_weight, hin_sugg)
            self._calculate_sim_scores(entry, train_set, 'ptp', 'pap', self.ptp_weight, hin_sugg)
        self._write_results(hin_sugg)


if __name__ == '__main__':
    enhanced_veto = PaperVeto.create_from_args()
    enhanced_veto.run()
