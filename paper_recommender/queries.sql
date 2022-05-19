db.papers.aggregate(
   [
     { $match: { $text: { $search: "recommendation" } } },
     { $project: { id:1, title: 1, _id: 0, abstract: 1, score: { $meta: "textScore" } } },
     { $sort: { score: { $meta: "textScore" } } },
     { $limit: 30 }
   ]
)