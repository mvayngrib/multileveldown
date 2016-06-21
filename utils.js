module.exports = {
  getEventTag: function getEventTag (event) {
    return event === 'put' ? 1 : event === 'del' ? 2 : 3
  }
}
