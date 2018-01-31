const Sequelize = require('sequelize');
const sequelize = require('../raw');

const VideoInfo = sequelize.define('videoinfo', {
  vid: { type: Sequelize.BIGINT, allowNull: false },
  vname: { type: Sequelize.STRING },
  videotype: { type: Sequelize.STRING },
  series: { type: Sequelize.INTEGER },
  updatenum: { type: Sequelize.INTEGER },
  duration: { type: Sequelize.BIGINT },
  storyplot: { type: Sequelize.STRING },
  issueyear: { type: Sequelize.INTEGER },
  studio: { type: Sequelize.STRING },
  language: { type: Sequelize.STRING },
  category: { type: Sequelize.STRING },
  area: { type: Sequelize.STRING },
  director: { type: Sequelize.STRING },
  star: { type: Sequelize.STRING },
  actor: { type: Sequelize.STRING },
  dubbing: { type: Sequelize.STRING },
  showhost: { type: Sequelize.STRING },
  showguest: { type: Sequelize.STRING },
  singer: { type: Sequelize.STRING },
  presenter: { type: Sequelize.STRING },
  taginfo: { type: Sequelize.STRING },
  score: { type: Sequelize.STRING },
  award: { type: Sequelize.STRING },
  goodsname: { type: Sequelize.STRING },
  goodsid: { type: Sequelize.STRING },
  goodsbrand: { type: Sequelize.STRING },
  goodsprice: { type: Sequelize.STRING },
}, {
  timestamps: false,  
  freezeTableName: true,
  tableName: 'videoinfo'
});

module.exports = VideoInfo;