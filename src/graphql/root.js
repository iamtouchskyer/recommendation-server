const {
  Filters,
  PlayEvents
} = require('./model');

const root = {
  filters: ({ videotype }) => {
    return new Filters(videotype);
  },
  playEvents: ({videotype, language, category, provinceID, startDate, endDate, area, hourOfDay, top}) => {
    return new PlayEvents(videotype, language, category, provinceID, startDate, endDate, area, hourOfDay, top);
  }
}

module.exports = root;