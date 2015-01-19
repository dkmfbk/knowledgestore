// Table sorting

$(function() {
	$(".tablesorter").tablesorter();
});

// Selection

if (window.location.href.indexOf("selection=") >= 0) {
	window.location.href = "#selection";
}

function select(id) {
	var url = window.location.href;
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

// URI handling (title tooltip, contextual menu)

var $uriMenu = $("#uriMenu");

// $uriMenu.on("mouseleave", function() {
// $uriMenu.hide();
// });

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
			+ "&selection=" + encodeURIComponent(selection);
	// $("#uriMenuURI").text(href);
	$("#uriMenuLookup").attr("href", lookupHref);
	$("#uriMenuLookupNewTab").attr("href", lookupHref);
	$("#uriMenuDereference").attr("href", href);
	$uriMenu.css({
		display : "block",
		left : e.pageX,
		top : e.pageY
	});
	return false;
}
