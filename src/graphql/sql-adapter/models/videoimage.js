const Sequelize = require('sequelize');
const sequelize = require('../raw');

const VideoImage = sequelize.define('videoimage', {
  fid: { type: Sequelize.STRING, allowNull: false },
  filename: { type: Sequelize.STRING },
  mime: { type: Sequelize.STRING },
  size: { type: Sequelize.INTEGER },
  mediatype: { type: Sequelize.INTEGER },
  imagemodel: { type: Sequelize.INTEGER },
  width: { type: Sequelize.INTEGER },
  height: { type: Sequelize.INTEGER },
  ctime: { type: Sequelize.DATE },
  mtime: { type: Sequelize.DATE },
  inuse: { type: Sequelize.INTEGER },
  cuid: { type: Sequelize.INTEGER },
  muid: { type: Sequelize.INTEGER },
  isallpackage: { type: Sequelize.INTEGER },
  cpid: { type: Sequelize.INTEGER },
  refvideo: { type: Sequelize.INTEGER },
}, {
  timestamps: false,  
  freezeTableName: true,
  tableName: 'videoimage'
});

module.exports = VideoImage;