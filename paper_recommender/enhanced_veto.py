import argparse
from pathlib import Path
import csv

# Define CSV dialect to be used.
csv.register_dialect(
    'exp_dialect',
    delimiter='\t'
)


class EnhancedVeto:
    """Wrapper class for running veto"""

    def __init__(self, expert_file, veto_output, apv_sims, apt_sims, sims_per_expert=50,
                 apv_weight=0.5, apt_weight=0.5, algorithm='borda', rrf_k=0, output_size=20):
        self.expert_file = expert_file
        self.veto_output = veto_output
        self.apv_sims = apv_sims
        self.apt_sims = apt_sims
        self.sims_per_expert = sims_per_expert
        self.apv_weight = apv_weight
        self.apt_weight = apt_weight
        self.algorithm = algorithm
        self.rrf_k = rrf_k
        self.output_size = output_size

    def __str__(self):
        return f'EnhancedVeto({id(self)})'

    @classmethod
    def create_from_args(cls):
        """Create from user arguments"""
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('-ef', '--expert_file', required=True, help='expert ids file')
        arg_parser.add_argument('-vo', '--veto_output', required=True, help='file where the results will be written')
        arg_parser.add_argument('-apv', '--apv_sims', required=True, help='directory containing the APV similarities')
        arg_parser.add_argument('-apt', '--apt_sims', required=True, help='directory containing the APT similarities')
        arg_parser.add_argument('-spe', '--sims_per_expert', nargs='?', type=int, default=50,
                                help='how many similarities per expert should be considered')
        arg_parser.add_argument('-apvw', '--apv_weight', nargs='?', type=float, default=0.5,
                                help='score weight for the APV similarities')
        arg_parser.add_argument('-aptw', '--apt_weight', nargs='?', type=float, default=0.5,
                                help='score weight for the APT similarities')
        arg_parser.add_argument('-algo', '--algorithm', nargs='?', default='borda',
                                help='the scoring algorithm to be used')
        arg_parser.add_argument('-rrfk', '--rrf_k', nargs='?', type=int, default=0,
                                help='rrf k algorithm ranking parameter')
        arg_parser.add_argument('-outs', '--output_size', nargs='?', type=int, default=20,
                                help='the size of the output')
        veto_args = arg_parser.parse_args()

        return cls(
            expert_file=veto_args.expert_file,
            veto_output=veto_args.veto_output,
            apv_sims=veto_args.apv_sims,
            apt_sims=veto_args.apt_sims,
            sims_per_expert=veto_args.sims_per_expert,
            apv_weight=veto_args.apv_weight,
            apt_weight=veto_args.apt_weight,
            algorithm=veto_args.algorithm,
            rrf_k=veto_args.rrf_k,
            output_size=veto_args.output_size
        )

    def score(self, coeff, lines_to_read, sim_score):
        """Scoring method"""
        if self.algorithm == 'borda':
            return coeff * lines_to_read
        elif self.algorithm == 'rrf':
            return coeff * (1.0 / (self.rrf_k + (self.sims_per_expert - lines_to_read)))
        elif self.algorithm == 'sum':
            return float(sim_score)

    def run(self):
        """The Run algorithm"""
        train_set = {}
        with open(self.expert_file) as train_file:
            train_entries = csv.reader(train_file, dialect='exp_dialect')
            for entry in train_entries:
                train_set[entry[0]] = 1

        # Get suggestions based on HIN
        hin_sugg = {}
        for entry in train_set:
            try:
                with open(Path(self.apt_sims, entry)) as auth_sim1_file:
                    sim1_authors = csv.reader(auth_sim1_file, dialect='exp_dialect')
                    lines_to_read = self.sims_per_expert
                    for auth in sim1_authors:
                        if auth[1] in train_set:  # do not consider anyone in the training set
                            continue
                        lines_to_read -= 1
                        if lines_to_read == -1:
                            break
                        if auth[1] in hin_sugg:
                            hin_sugg[auth[1]]['apt'] += self.score(self.apt_weight, lines_to_read, auth[2])
                        else:
                            hin_sugg[auth[1]] = {}
                            hin_sugg[auth[1]]['apv'] = 0
                            hin_sugg[auth[1]]['apt'] = self.score(self.apt_weight, lines_to_read, auth[2])

                    auth_sim1_file.close()
            except FileNotFoundError:
                pass

            try:
                with open(Path(self.apv_sims, entry)) as auth_sim2_file:
                    sim2_authors = csv.reader(auth_sim2_file, dialect='exp_dialect')
                    lines_to_read = self.sims_per_expert
                    for auth in sim2_authors:
                        if auth[1] in train_set:  # do not consider anyone in the training set
                            continue
                        lines_to_read -= 1
                        if lines_to_read == -1:
                            break
                        if auth[1] in hin_sugg:
                            hin_sugg[auth[1]]['apv'] += self.score(self.apv_weight, lines_to_read, auth[2])
                        else:
                            hin_sugg[auth[1]] = {}
                            hin_sugg[auth[1]]['apv'] = self.score(self.apv_weight, lines_to_read, auth[2])
                            hin_sugg[auth[1]]['apt'] = 0

                    auth_sim2_file.close()
            except FileNotFoundError:
                pass

        for sugg in hin_sugg.keys():
            hin_sugg[sugg]['overall'] = hin_sugg[sugg]['apt'] + hin_sugg[sugg]['apv']

        hin_sugg_list = sorted(hin_sugg, key=lambda k: hin_sugg[k]['overall'],
                               reverse=True)  # sort suggestions based on borda count
        hin_sugg_list = hin_sugg_list[0:self.output_size]  # keep as many as in the test size

        with open(self.veto_output, 'w', newline='') as hin_sugg_file:
            hin_sugg_writer = csv.writer(hin_sugg_file)
            for sugg in hin_sugg_list:
                hin_sugg_writer.writerow([sugg,
                                          round(hin_sugg[sugg]['overall'], 2),
                                          round(hin_sugg[sugg]['apt'], 2),
                                          round(hin_sugg[sugg]['apv'], 2)]
                                         )


if __name__ == '__main__':
    enhanced_veto = EnhancedVeto.create_from_args()
    enhanced_veto.run()
