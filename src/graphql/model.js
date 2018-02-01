const _ = require('lodash');
const { extractUniqueValues, runSproc } = require('./sql-adapter/index');
const { videoInfoLoader } = require('./cache');

class RankResult {
  constructor(vid, videoname, play_count) {
    this.vid = vid;
    this.videoname = videoname;
    this.play_count = play_count;
  }
}

class CountResult {
  constructor(playHour, playCount) {
    const hourObj = new Date(playHour);
    this.playHour = hourObj.getFullYear() + '-' + (1 + hourObj.getMonth()) + '-' + hourObj.getDate() + ' ' + hourObj.getUTCHours() + ':00:00';
    this.playCount = playCount;
  }
}

class PlayEvents {
  constructor(videotype, language, category, provinceID, startDate, endDate, area, hourOfDay) {
    this.videotype = videotype;
    this.language = language;
    this.category = category;
    this.provinceID = provinceID || -1;
    this.area = area;
    this.hourOfDay = hourOfDay || 25;
    this.startDate = startDate;

    const currentDate = new Date(Date.now());
    this.endDate = endDate || currentDate.getFullYear() + '-' + (1 + currentDate.getMonth()) + '-' + currentDate.getDate();
  }

  rank({ top }) {
    top = top || 10;
    let params = {
      'videoType': '\'' + this.videotype + '\'',
      'language': _.isUndefined(this.language) ? 'NULL' : 'N\'' + this.language + '\'',
      'category': _.isUndefined(this.category) ? 'NULL' : 'N\'' + this.category + '\'',
      'area': _.isUndefined(this.area) ? 'NULL' : 'N\'' + this.area + '\'',
      'hourOfDay': this.hourOfDay,
      'provinceID': this.provinceID,
      'startDate': '\'' + this.startDate + '\'',
      'endDate': '\'' + this.endDate + '\'',
      'top': top,
    };

    let rank = runSproc('prc_rankVideoByPlayCount', params, true);

    return rank.then(response => {
      let ret = [];
      response.forEach(element => {
        let r = new RankResult(element.vid, element.videoname, element.play_count);

        ret.push(r);
      });

      return ret;
    });
  }

  count() {
    let params = {
      'videoType': _.isUndefined(this.videotype) ? 'NULL' : '\'' + this.videotype + '\'',
      'provinceID': this.provinceID,
      'startDate': '\'' + this.startDate + '\'',
      'endDate': '\'' + this.endDate + '\'',
    };

    let count = runSproc('prc_playCountByHour', params, true);

    return count.then(response => {
      let ret = [];
      response.forEach(element => {
        ret.push(new CountResult(element.PlayHour, element.PlayCount));
      });

      return ret;
    });
  }
}

class Filters {
  constructor(videotype) {
    this.videotype = videotype;
  }

  languages() {
    return videoInfoLoader.load({
      videotype: this.videotype,
      column: 'language'
    });
  }

  areas() {
    return videoInfoLoader.load({
      videotype: this.videotype,
      column: 'area'
    });
  }

  categories() {
    return videoInfoLoader.load({
      videotype: this.videotype,
      column: 'category'
    });
  }
}

module.exports = {
  Filters,
  PlayEvents
};