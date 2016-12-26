// MIT License
// (c) 2013-2014 Laurent Debacker

// Usage:
// var filter = QM.And(QM.Not(QM.Eq("type",[QM.String("foo"),QM.String("bar")])),QM.Fts("text","belgian chocolate"));
// var sort = QM.Sort(QM.Order("rooms",false),QM.Order("price"));
// window.location.search = "?f=" + filter + "&s=" + sort;

var QM = function() {
	function escapeChar(c) {
		var s = c.charCodeAt(0).toString(16);
		return '%' + (s.length == 1 ? '0' + s : s);
	}

	function escapeString(s) {
		return encodeURIComponent(s).replace(/[()]/g, escapeChar);
	}

	return {
		Sort: function(orders) {
			if(orders.constructor != Array)
				orders = (arguments.length === 1?[arguments[0]]:Array.apply(null, arguments));
			return Array.prototype.join.call(orders, "");
		},

		Order: function(field, ascending) {
			field = escapeString(field);
			if(ascending === false)
				return '!' + field;
			return field;
		},

		Not: function(predicate) {
			return "not(" + predicate + ")";
		},

		And: function(predicates) {
			if(predicates.constructor != Array)
				predicates = (arguments.length === 1?[arguments[0]]:Array.apply(null, arguments));
			return "and(" + predicates.join() + ")";
		},

		Or: function(predicates) {
			if(predicates.constructor != Array)
				predicates = (arguments.length === 1?[arguments[0]]:Array.apply(null, arguments));
			return "or(" + predicates.join() + ")";
		},

		Eq: function (field, values) {
			if(values.constructor != Array)
				values = Array.prototype.splice.call(arguments, 1);
			var s = "eq(" + escapeString(field);
			if(values.length) s += "," + values.join();
			return s + ")";
		},

		Lt: function(field, value) {
			return "lt(" + escapeString(field) + "," + value + ")";
		},

		Le: function(field, value) {
			return "le(" + escapeString(field) + "," + value + ")";
		},

		Gt: function(field, value) {
			return "gt(" + escapeString(field) + "," + value + ")";
		},

		Ge: function(field, value) {
			return "ge(" + escapeString(field) + "," + value + ")";
		},

		Fts: function(field, string) {
			return "fts(" + escapeString(field) + "," + QM.String(string) + ")";
		},

		Null: "null",

		Boolean: function(bool) {
			if(bool === null) return QM.Null;
			return bool ? "true" : "false";
		},

		Number: function(number) {
			if(number === null) return QM.Null;
			return "" + number;
		},

		String: function(string) {
			if(string === null) return QM.Null;
			return "$" + escapeString(string);
		},

		Date: function(date) {
			if(date === null) return QM.Null;
			return date.toJSON();
		}
	};
} ();
