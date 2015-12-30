String.prototype.endsWith = function(suffix) {
    return this.indexOf(suffix, this.length - suffix.length) !== -1;
};

function select(id) {
	var url = window.location.href;
    if (url.endsWith("#")) {
        url = url.substr(0, url.length - 1);
    }
	var start = url.indexOf("selection=");
	if (start >= 0) {
		start += 10;
		var end = url.indexOf("&", start);
		url = url.substr(0, start) + encodeURIComponent(id)
				+ (end <= 0 ? "" : url.substr(end));
	} else if (url.indexOf("?") >= 0) {
		url = url + "&selection=" + encodeURIComponent(id);
	} else {
		url = url + "?selection=" + encodeURIComponent(id);
	}
	window.location.href = url;
}


function applyDataTable(tableId, toggle, options) {
  var table = $('#' + tableId);
  if (toggle) {
    var index = 0;
    var html = "<div id='toggle_" + tableId + "'>Toggle columns: |";
    table.find('th').each(function() { html = html + " <a class='toggle-vis' data-column='" + (index++) + "'>" + $(this).text() + "</a> |"; });
    html += "</div>";
    table.before(html);
  }
  var head = table.find('thead');
  var filters = head.clone().attr('class', 'datatable-filter');
  head.attr('class', 'datatable-head');
  filters.insertAfter(head);
  $('th', filters).html('<small><input type="text" style="width:100%; height: 2em; font-weight: normal; font-size: 90%" /></small>');
  var dtable = table.DataTable($.extend({}, { "dom": 'iRlfrtp', paging: false, scrollY: 'calc(100vh - 250px)', scrollCollapse: true, scrollX: true }, options));
  dtable.columns().eq(0).each(function(colIdx) {
    $('input', $('th', filters)[colIdx]).on('keyup change', function() {
      dtable.column(colIdx).search(this.value).draw();
    });
  });
  if (toggle) {
    $('#toggle_' + tableId + ' a.toggle-vis').on('click', function(e) {
      e.preventDefault();
      var column = dtable.column(':contains("' + $(this).text() + '")');
      column.visible(!column.visible());
    });
  }
  return dtable;
}

function applyDataTable(tableId, toggle, options) {
  var table = $('#' + tableId);
  if (toggle) {
    var index = 0;
    var html = "<div id='toggle_" + tableId + "'>Toggle columns: |";
    table.find('th').each(function() { html = html + " <a class='toggle-vis' data-column='" + (index++) + "'>" + $(this).text() + "</a> |"; });
    html += "</div>";
    table.before(html);
  }
  var head = table.find('thead tr');
  var filters = head.clone().attr('class', 'datatable-filter');
  filters.insertAfter(head);
  head.attr('class', 'datatable-head');
  $('th', filters).html('<small><input type="text" style="width:100%; height: 2em; font-weight: normal; font-size: 90%" /></small>');
  var dtable = table.DataTable($.extend({}, { "dom": 'iRlfrtp', sortCellsTop: true, paging: false, scrollY: 'calc(100vh - 250px)', scrollCollapse: true, scrollX: true }, options));
  dtable.columns().eq(0).each(function(colIdx) {
    $('input', $('th', filters)[colIdx]).on('keyup change', function() {
      dtable.column(colIdx).search(this.value).draw();
    });
  });
  if (toggle) {
    $('#toggle_' + tableId + ' a.toggle-vis').on('click', function(e) {
      e.preventDefault();
      var column = dtable.column(':contains("' + $(this).text() + '")');
      column.visible(!column.visible());
    });
  }
  return dtable;
}

$(document).ready(function() {

  // Table sorting
  $(function() {
	$(".tablesorter").tablesorter();
  });
  
  //Selection

  //commented as it doesn't work properly
  //if (window.location.href.indexOf("selection=") >= 0) {
  //	window.location.href = "#selection";
  //}

  //URI handling (title tooltip, contextual menu)

  var $uriMenu = $("#uriMenu");

  //$uriMenu.on("mouseleave", function() {
  //$uriMenu.hide();
  //});

  $(document).click(function() {
	$uriMenu.hide();
  });

  $("a.uri").each(function() {
	$(this).attr("title", $(this).attr("href"));
  });

  $("body").on("click", "a.uri", uriMenuSetup);
  function uriMenuSetup(e) {
	var $anchor = $(this);
	var href = $anchor.attr("href");
	var selection = $anchor.data("sel");
	var lookupHref = "ui?action=lookup&id=" + encodeURIComponent(href)
			+ (selection == null ? "" : "&selection=" + encodeURIComponent(selection));
	var entityMentionsHref = "ui?action=entity-mentions&entity=" + encodeURIComponent(href);
	var entityMentionsAggregateHref = "ui?action=entity-mentions-aggregate&entity=" + encodeURIComponent(href);
	// $("#uriMenuURI").text(href);
	$("#uriMenuLookup").attr("href", lookupHref);
	$("#uriMenuLookupNewTab").attr("href", lookupHref);
	$("#uriMenuDereference").attr("href", href);
	$("#uriMenuEntityMentions").attr("href", entityMentionsHref);
	$("#uriMenuEntityMentionsAggregate").attr("href", entityMentionsAggregateHref);
	$uriMenu.css({
		display : "block",
		left : e.pageX,
		top : e.pageY
	});
	return false;
  }

});
