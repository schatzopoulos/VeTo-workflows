db.papers.aggregate(
   [
     { $match: { $text: { $search: "Ranking scientific publications with similarity-preferential mechanism" } } },
     { $project: { id:1, title: 1, _id: 0, abstract: 1, rscore: { $round: [{ $meta: "textScore" }, 2] }, year: 1 } },
     { $sort: { score: { $meta: "textScore" } } },
     { $limit: 20 }
   ]
)

db.aminer_mapper.aggregate(
   [   { $match: { id: '2657651'} },
       { $lookup: {
            from: "papers",
            localField: "aminer_id",
            foreignField: "id",
           as: "mapping"
            }
         },
         { $unwind: "$mapping" },
         { $replaceRoot: { newRoot: { title: "$mapping.title" } } },
         { $project: {_id: 0, title: 1} },
       ]
)