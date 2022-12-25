import math

rating = '''
2
2
2
2
2
2
2
2
2
2
2
2
2
2
1
2
1
2
1
2
'''


def ndcg(n, ranks, rels):
    """
    Computes the normalized discounted cumulative gain (nDCG) at a position `n`.

    Args:
    n: The position at which to compute nDCG.
    ranks: A list of ranks, where rank i corresponds to the rank of the item with relevance score rels[i].
    rels: A list of relevance scores, where rels[i] is the relevance score of the item at rank i.

    Returns:
    The nDCG at the position `n`.
    """
    dcg = 0
    for i in range(n):
        dcg += (2 ** rels[i] - 1) / math.log2(ranks[i] + 1)
    idcg = 0
    rels = sorted(rels, reverse=True)
    for i in range(n):
        idcg += (2 ** rels[i] - 1) / math.log2(i + 2)
    return (dcg / idcg)


def mean_rating(n, ratings):
    """
    Computes the mean rating at a position `n`.

    Args:
    n: The position at which to compute the mean rating.
    ratings: A list of ratings, where ratings[i] is the rating of the item at rank i.

    Returns:
    The mean rating at the position `n`.
    """
    return sum(ratings[:n]) / n


def main():
    rels = [int(i) for i in rating.strip().split('\n')]
    ranks = [i for i in range(1, len(rels) + 1)]
    print(f'Relevance: {rels}')
    print(f'Ranks: {ranks}')
    assert len(rels) == len(ranks)
    res = []
    res_2 = []
    for n in (1, 5, 10, 20):
        res.append(round(ndcg(n, ranks, rels), 2))
        res_2.append(round(mean_rating(n, rels), 2))
    res.extend(res_2)
    print('\t'.join(str(item) for item in res))


if __name__ == '__main__':
    main()
