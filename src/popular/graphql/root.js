const { Filters, PlayCount } = require('./model');

const root = {
  filters: ({ videotype }) => {
    return new Filters(videotype);
  },
  playCount: ({videotype, language, category, provinceID, startDate, endDate, area, hourOfDay, top}) => {
    return new PlayCount(videotype, language, category, provinceID, startDate, endDate, area, hourOfDay, top);
  }
}

module.exports = root;