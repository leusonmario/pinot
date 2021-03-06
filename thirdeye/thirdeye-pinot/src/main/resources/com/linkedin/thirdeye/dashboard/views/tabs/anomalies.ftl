
<div class="container-fluid">
  <div class="row bg-white row-bordered ">
    <div class="container">

      <div class="search-bar">
        <div class="search-select">
          <select id="anomalies-search-mode" style="width:100%">
            <option value="metric">Metric(s)</option>
            <option value="dashboard">Dashboard</option>
            <option value="id">Anomaly ID</option>
            <option value="time" selected>Time</option>
          </select>
        </div>

        <div class="search-input search-field">
          <div id="anomalies-search-metrics-container" class="" style="overflow:hidden; display: none;">
            <select id="anomalies-search-metrics-input" class="label-large-light" multiple="multiple"></select>
          </div>
          <div id="anomalies-search-dashboard-container" class=""  style="overflow:hidden; display: none;">
            <select id="anomalies-search-dashboard-input" class="label-large-light"></select>
          </div>
          <div id="anomalies-search-anomaly-container" class=""  style="overflow:hidden; display: none;">
            <select id="anomalies-search-anomaly-input" class="label-large-light" multiple="multiple"></select>
          </div>
          <div id="anomalies-search-time-container" class=""  style="overflow:hidden; display: none;">
            <select id="anomalies-search-time-input" class="label-large-light"></select>
          </div>
        </div>

        <a class="btn thirdeye-btn search-button" type="button" id="search-button"><span class="glyphicon glyphicon-search"></span></a>

      </div>
    </div>
  </div>
</div>

<div class="container top-buffer bottom-buffer">
  <div class="page-content">
    <div class="page-filter">
      <div class="anomalies-panel">
        <div class="filter-header">
          Filter
        </div>

        <div class="filter-body">
          <div id="#anomalies-time-range">
            <div class="datepicker-field">
              <h5 class="label-medium-semibold">Start date</label>
              <div id="anomalies-time-range-start" class="datepicker-range">
                <span></span>
                <b class="caret"></b>
              </div>
            </div>
            <div class="datepicker-field">
              <h5 class="label-medium-semibold">End date</label>
              <div id="anomalies-time-range-end" class="datepicker-range">
                <span></span>
                <b class="caret"></b>
              </div>
            </div>
          </div>
        </div>

        <div class="filter-footer">
          <a type="button" id="apply-button" class="thirdeye-link">Apply Filter</a>
        </div>
      </div>
    </div>
    <div id='anomaly-spin-area'></div>
    <div class="page-results" id="anomaly-results-place-holder"></div>
  </div>
</div>
