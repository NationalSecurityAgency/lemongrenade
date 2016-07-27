
module.exports = function(app) {
    app.use('/jobs', require('./jobs.js'));
};

