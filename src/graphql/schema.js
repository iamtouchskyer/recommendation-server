const _ = require('lodash');
const { graphql, buildSchema } = require('graphql');

const schema = buildSchema(`
  type Filters {
    languages: [String],
    areas: [String],
    categories: [String]
  }

  type RankResult {
    vid: Int!,
    videoname: String!,
    play_count: Int!
  }

  type PlayEvents {
    rank(top: Int): [RankResult]!,
    count: [CountResult]!
  }

  type CountResult {
    playHour: String!,
    playCount: Int!
  }

  type Query {
    filters(videotype: String): Filters,

    playEvents (
      videotype: String, 
      language: String,
      category: String,
      provinceID: Int,
      startDate: String,
      endDate: String,
      area: String,
      hourOfDay: Int,
    ): PlayEvents
  }
`)

module.exports = schema;

