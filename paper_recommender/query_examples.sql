db.papers.aggregate(
   [
     { $match: { id: { $nin: [ 'ac88c4da-6a31-4ed2-a92d-e03e2cc64cc0', '0dd5868a-0b79-44f7-8713-2a42314ffd5a' ] },
     $text: { $search: "How different connectivity patterns of individuals within an organization can speed up organizational learning" } } },
     { $project: { id: 1, title: 1, _id: 0, abstract: 1, rscore: { $round: [{ $meta: "textScore" }, 2] }, year: 1 } },
     { $sort: { score: { $meta: "textScore" } } },
     { $limit: 20 },
     { $lookup: {
            from: "aminer_mapper",
            localField: "id",
            foreignField: "aminer_id",
           as: "mapping"
            }
         },
     { $unwind: "$mapping" },
     { $replaceRoot: { newRoot: { id: "$mapping.id" } } },
     { $project: {'_id': 0, 'id': 1}}
   ]
)

db.aminer_mapper.aggregate(
   [   { $match: { id: { $in: ['929187', '1306030', '1108957'] } } },
       { $lookup: {
            from: "papers",
            localField: "aminer_id",
            foreignField: "id",
           as: "mapping"
            }
         },
         { $unwind: "$mapping" },
         { $replaceRoot: { newRoot: { title: "$mapping.title", aminer_id: "$mapping.id" } } },
         { $project: {'_id': 0, 'title': 1, 'aminer_id': 1}},
       ]
)

db.aminer_mapper.aggregate(
   [   { $match: { id: { $in:
                                ['929187']
   } } },
       { $lookup: {
            from: "papers",
            localField: "aminer_id",
            foreignField: "id",
           as: "mapping"
            }
         },
         { $unwind: "$mapping" },
         { $replaceRoot: { newRoot: {id: "$id", title: "$mapping.title", aminer_id: "$mapping.id", abstract: "$mapping.abstract", year: "$mapping.year" } } },
         { $project: {'_id': 0, 'title': 1, 'aminer_id': 1, 'id': 1, 'year': 1, 'abstract': 1}},
       ]
)

db.papers.aggregate(
   [
     { $match: {
     $text: { $search: "The citation wake of publications detects Nobel" } } },
     { $project: { id: 1, title: 1, _id: 0, abstract: 1, rscore: { $round: [{ $meta: "textScore" }, 2] }, year: 1 } },
     { $sort: { score: { $meta: "textScore" } } },
     { $limit: 20 }
   ]
)
