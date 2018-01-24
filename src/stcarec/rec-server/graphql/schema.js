const _ = require('lodash');
const { graphql, buildSchema } = require('graphql');

const schema = buildSchema(`
  type Filters {
    languages: [String],
    areas: [String],
    categories: [String]
  }

  type VideoCountEntity {
    vid: Int!,
    videoname: String!,
    play_count: Int!
  }

  type PlayCount {
    count(top: Int): [VideoCountEntity]!
  }

  type Query {
    filters(videotype: String): Filters,

    playCount(
      videotype: String!, 
      language: String,
      category: String,
      provinceID: Int,
      startDate: String!,
      endDate: String,
      area: String,
      hourOfDay: Int,
    ): PlayCount
  }
`)

module.exports = schema;

