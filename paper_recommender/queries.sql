db.papers.aggregate(
   [
     { $match: { $text: { $search: "Ranking scientific publications with similarity-preferential mechanism" } } },
     { $project: { id:1, title: 1, _id: 0, abstract: 1, rscore: { $round: [{ $meta: "textScore" }, 2] }, year: 1 } },
     { $sort: { score: { $meta: "textScore" } } },
     { $limit: 20 }
   ]
)