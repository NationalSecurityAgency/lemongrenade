/**
 * jQRangeSlider
 * A javascript slider selector that supports dates
 *
 * Copyright (C) Guillaume Gautreau 2012
 * Dual licensed under the MIT or GPL Version 2 licenses.
 */

(function($, undefined){

	"use strict";

	$.widget("ui.rangeSliderMouseTouch", $.ui.mouse, {
		enabled: true,

		_mouseInit: function(){
			var that = this;
			$.ui.mouse.prototype._mouseInit.apply(this);
			this._mouseDownEvent = false;

			this.element.bind('touchstart.' + this.widgetName, function(event) {
				return that._touchStart(event);
			});
		},

		_mouseDestroy: function(){
			$(document)
				.unbind('touchmove.' + this.widgetName, this._touchMoveDelegate)
				.unbind('touchend.' + this.widgetName, this._touchEndDelegate);

			$.ui.mouse.prototype._mouseDestroy.apply(this);
		},

		enable: function(){
			this.enabled = true;
		},

		disable: function(){
			this.enabled = false;
		},

		destroy: function(){
			this._mouseDestroy();

			$.ui.mouse.prototype.destroy.apply(this);

			this._mouseInit = null;
		},

		_touchStart: function(event){
			if (!this.enabled) return false;

			event.which = 1;
			event.preventDefault();

			this._fillTouchEvent(event);

			var that = this,
				downEvent = this._mouseDownEvent;

			this._mouseDown(event);

			if (downEvent !== this._mouseDownEvent){

				this._touchEndDelegate = function(event){
					that._touchEnd(event);
				}

				this._touchMoveDelegate = function(event){
					that._touchMove(event);
				}

				$(document)
				.bind('touchmove.' + this.widgetName, this._touchMoveDelegate)
				.bind('touchend.' + this.widgetName, this._touchEndDelegate);
			}
		},

		_mouseDown: function(event){
			if (!this.enabled) return false;

			return $.ui.mouse.prototype._mouseDown.apply(this, [event]);
		},

		_touchEnd: function(event){
			this._fillTouchEvent(event);
			this._mouseUp(event);

			$(document)
			.unbind('touchmove.' + this.widgetName, this._touchMoveDelegate)
			.unbind('touchend.' + this.widgetName, this._touchEndDelegate);

		this._mouseDownEvent = false;

		// No other choice to reinitialize mouseHandled
		$(document).trigger("mouseup");
		},

		_touchMove: function(event){
			event.preventDefault();
			this._fillTouchEvent(event);

			return this._mouseMove(event);
		},

		_fillTouchEvent: function(event){
			var touch;

			if (typeof event.targetTouches === "undefined" && typeof event.changedTouches === "undefined"){
				touch = event.originalEvent.targetTouches[0] || event.originalEvent.changedTouches[0];
			} else {
				touch = event.targetTouches[0] || event.changedTouches[0];
			}

			event.pageX = touch.pageX;
			event.pageY = touch.pageY;

			event.which = 1; // [~] fixes touch events for jquery-ui.1.11.x.mouse
		}
	});
}(jQuery));
/**
 * jQRangeSlider
 * A javascript slider selector that supports dates
 *
 * Copyright (C) Guillaume Gautreau 2012
 * Dual licensed under the MIT or GPL Version 2 licenses.
 */

(function($, undefined){
	"use strict";

	$.widget("ui.rangeSliderDraggable", $.ui.rangeSliderMouseTouch, {
		cache: null,

		options: {
			containment: null
		},

		_create: function(){
			$.ui.rangeSliderMouseTouch.prototype._create.apply(this);

			setTimeout($.proxy(this._initElementIfNotDestroyed, this), 10);
		},

		destroy: function(){
			this.cache = null;
			
			$.ui.rangeSliderMouseTouch.prototype.destroy.apply(this);
		},

		_initElementIfNotDestroyed: function(){
			if (this._mouseInit){
				this._initElement();
			}
		},

		_initElement: function(){
			this._mouseInit();
			this._cache();
		},

		_setOption: function(key, value){
			if (key === "containment"){
				if (value === null || $(value).length === 0){
					this.options.containment = null
				}else{
					this.options.containment = $(value);
				}
			}
		},

		/*
		 * UI mouse widget
		 */

		_mouseStart: function(event){
			this._cache();
			this.cache.click = {
					left: event.pageX,
					top: event.pageY
			};

			this.cache.initialOffset = this.element.offset();

			this._triggerMouseEvent("mousestart");

			return true;
		},

		_mouseDrag: function(event){
			var position = event.pageX - this.cache.click.left;

			position = this._constraintPosition(position + this.cache.initialOffset.left);

			this._applyPosition(position);

			this._triggerMouseEvent("sliderDrag");

			return false;
		},

		_mouseStop: function(){
			this._triggerMouseEvent("stop");
		},

		/*
		 * To be overriden
		 */

		_constraintPosition: function(position){
			if (this.element.parent().length !== 0 && this.cache.parent.offset !== null){
				position = Math.min(position, 
					this.cache.parent.offset.left + this.cache.parent.width - this.cache.width.outer);
				position = Math.max(position, this.cache.parent.offset.left);
			}

			return position;
		},

		_applyPosition: function(position){
			var offset = {
				top: this.cache.offset.top,
				left: position
			}

			this.element.offset({left:position});

			this.cache.offset = offset;
		},

		/*
		 * Private utils
		 */

		_cacheIfNecessary: function(){
			if (this.cache === null){
				this._cache();
			}
		},

		_cache: function(){
			this.cache = {};

			this._cacheMargins();
			this._cacheParent();
			this._cacheDimensions();

			this.cache.offset = this.element.offset();
		},

		_cacheMargins: function(){
			this.cache.margin = {
				left: this._parsePixels(this.element, "marginLeft"),
				right: this._parsePixels(this.element, "marginRight"),
				top: this._parsePixels(this.element, "marginTop"),
				bottom: this._parsePixels(this.element, "marginBottom")
			};
		},

		_cacheParent: function(){
			if (this.options.parent !== null){
				var container = this.element.parent();

				this.cache.parent = {
					offset: container.offset(),
					width: container.width()
				}
			}else{
				this.cache.parent = null;
			}
		},

		_cacheDimensions: function(){
			this.cache.width = {
				outer: this.element.outerWidth(),
				inner: this.element.width()
			}
		},

		_parsePixels: function(element, string){
			return parseInt(element.css(string), 10) || 0;
		},

		_triggerMouseEvent: function(event){
			var data = this._prepareEventData();

			this.element.trigger(event, data);
		},

		_prepareEventData: function(){
			return {
				element: this.element,
				offset: this.cache.offset || null
			};
		}
	});

}(jQuery));/**
 * jQRangeSlider
 * A javascript slider selector that supports dates
 *
 * Copyright (C) Guillaume Gautreau 2012
 * Dual licensed under the MIT or GPL Version 2 licenses.
 *
 */


 (function($, undefined){
	"use strict";

	$.widget("ui.rangeSliderHandle", $.ui.rangeSliderDraggable, {
		currentMove: null,
		margin: 0,
		parentElement: null,

		options: {
			isLeft: true,
			bounds: {min:0, max:100},
			range: false,
			value: 0,
			step: false
		},

		_value: 0,
		_left: 0,

		_create: function(){
			$.ui.rangeSliderDraggable.prototype._create.apply(this);

			this.element
				.css("position", "absolute")
				.css("top", 0)
				.addClass("ui-rangeSlider-handle")
				.toggleClass("ui-rangeSlider-leftHandle", this.options.isLeft)
				.toggleClass("ui-rangeSlider-rightHandle", !this.options.isLeft);

			this.element.append("<div class='ui-rangeSlider-handle-inner' />");

			this._value = this._constraintValue(this.options.value);
		},

		destroy: function(){
			this.element.empty();	

			$.ui.rangeSliderDraggable.prototype.destroy.apply(this);			
		},

		_setOption: function(key, value){
			if (key === "isLeft" && (value === true || value === false) && value !== this.options.isLeft){
				this.options.isLeft = value;

				this.element
					.toggleClass("ui-rangeSlider-leftHandle", this.options.isLeft)
					.toggleClass("ui-rangeSlider-rightHandle", !this.options.isLeft);

				this._position(this._value);

				this.element.trigger("switch", this.options.isLeft);
			} else if (key === "step" && this._checkStep(value)){
				this.options.step = value;
				this.update();
			} else if (key === "bounds"){
				this.options.bounds = value;
				this.update();
			}else if (key === "range" && this._checkRange(value)){
				this.options.range = value;
				this.update();
			}else if (key === "symmetricPositionning"){
				this.options.symmetricPositionning = value === true;
				this.update();
			}

			$.ui.rangeSliderDraggable.prototype._setOption.apply(this, [key, value]);
		},

		_checkRange: function(range){
			return range === false || (!this._isValidValue(range.min) && !this._isValidValue(range.max));
		},

		_isValidValue: function(value){
			return typeof value !== "undefined" && value !== false && parseFloat(value) !== value;
		},

		_checkStep: function(step){
			return (step === false || parseFloat(step) === step);
		},

		_initElement: function(){
			$.ui.rangeSliderDraggable.prototype._initElement.apply(this);
			
			if (this.cache.parent.width === 0 || this.cache.parent.width === null){
				setTimeout($.proxy(this._initElementIfNotDestroyed, this), 500);
			}else{
				this._position(this._value);
				this._triggerMouseEvent("initialize");
			}
		},

		_bounds: function(){
			return this.options.bounds;
		},

		/*
		 * From draggable
		 */

		_cache: function(){
			$.ui.rangeSliderDraggable.prototype._cache.apply(this);

			this._cacheParent();
		},

		_cacheParent: function(){
			var parent = this.element.parent();

			this.cache.parent = {
				element: parent,
				offset: parent.offset(),
				padding: {
					left: this._parsePixels(parent, "paddingLeft")
				},
				width: parent.width()
			}
		},

		_position: function(value){
			var left = this._getPositionForValue(value);

			this._applyPosition(left);
		},

		_constraintPosition: function(position){
			var value = this._getValueForPosition(position);

			return this._getPositionForValue(value);
		},

		_applyPosition: function(left){
			$.ui.rangeSliderDraggable.prototype._applyPosition.apply(this, [left]);

			this._left = left;
			this._setValue(this._getValueForPosition(left));
			this._triggerMouseEvent("moving");
		},

		_prepareEventData: function(){
			var data = $.ui.rangeSliderDraggable.prototype._prepareEventData.apply(this);

			data.value = this._value;

			return data;
		},

		/*
		 * Value
		 */
		_setValue: function(value){
			if (value !== this._value){
				this._value = value;
			}
		},

		_constraintValue: function(value){
			value = Math.min(value, this._bounds().max);
			value = Math.max(value, this._bounds().min);
		
			value = this._round(value);

			if (this.options.range !== false){
				var min = this.options.range.min || false,
					max = this.options.range.max || false;

				if (min !== false){
					value = Math.max(value, this._round(min));
				}

				if (max !== false){
					value = Math.min(value, this._round(max));
				}

				value = Math.min(value, this._bounds().max);
				value = Math.max(value, this._bounds().min);
			}

			return value;
		},

		_round: function(value){
			if (this.options.step !== false && this.options.step > 0){
				return Math.round(value / this.options.step) * this.options.step;
			}

			return value;
		},

		_getPositionForValue: function(value){
			if (!this.cache || !this.cache.parent || this.cache.parent.offset === null){
				return 0;
			}

			value = this._constraintValue(value);

			var ratio = (value - this.options.bounds.min) / (this.options.bounds.max - this.options.bounds.min),
				availableWidth = this.cache.parent.width,
				parentPosition = this.cache.parent.offset.left,
				shift = this.options.isLeft ? 0 : this.cache.width.outer;


			if (!this.options.symmetricPositionning){
				return ratio * availableWidth + parentPosition - shift;
			}

			return ratio * (availableWidth - 2 * this.cache.width.outer) + parentPosition + shift;
		},

		_getValueForPosition: function(position){
			var raw = this._getRawValueForPositionAndBounds(position, this.options.bounds.min, this.options.bounds.max);

			return this._constraintValue(raw);
		},

		_getRawValueForPositionAndBounds: function(position, min, max){

			var parentPosition =  this.cache.parent.offset === null ? 0 : this.cache.parent.offset.left,
					availableWidth,
					ratio;

			if (this.options.symmetricPositionning){
				position -= this.options.isLeft ? 0 : this.cache.width.outer;	
				availableWidth = this.cache.parent.width - 2 * this.cache.width.outer;
			}else{
				position += this.options.isLeft ? 0 : this.cache.width.outer;	
				availableWidth = this.cache.parent.width;
			}

			if (availableWidth === 0){
				return this._value;
			}

			ratio = (position - parentPosition) / availableWidth;

			return	ratio * (max - min) + min;
		},

		/*
		 * Public
		 */

		value: function(value){
			if (typeof value !== "undefined"){
				this._cache();

				value = this._constraintValue(value);

				this._position(value);
			}

			return this._value;
		},

		update: function(){
			this._cache();
			var value = this._constraintValue(this._value),
				position = this._getPositionForValue(value);

			if (value !== this._value){
				this._triggerMouseEvent("updating");
				this._position(value);
				this._triggerMouseEvent("update");
			}else if (position !== this.cache.offset.left){
				this._triggerMouseEvent("updating");
				this._position(value);
				this._triggerMouseEvent("update");
			}
		},

		position: function(position){
			if (typeof position !== "undefined"){
				this._cache();
				
				position = this._constraintPosition(position);
				this._applyPosition(position);
			}

			return this._left;
		},

		add: function(value, step){
			return value + step;
		},

		substract: function(value, step){
			return value - step;
		},

		stepsBetween: function(val1, val2){
			if (this.options.step === false){
				return val2 - val1;
			}

			return (val2 - val1) / this.options.step;
		},

		multiplyStep: function(step, factor){
			return step * factor;
		},

		moveRight: function(quantity){
			var previous;

			if (this.options.step === false){
				previous = this._left;
				this.position(this._left + quantity);

				return this._left - previous;
			}
			
			previous = this._value;
			this.value(this.add(previous, this.multiplyStep(this.options.step, quantity)));

			return this.stepsBetween(previous, this._value);
		},

		moveLeft: function(quantity){
			return -this.moveRight(-quantity);
		},

		stepRatio: function(){
			if (this.options.step === false){
				return 1;
			}else{
				var steps = (this.options.bounds.max - this.options.bounds.min) / this.options.step;
				return this.cache.parent.width / steps;
			}
		}
	});
 }(jQuery));
/**
 * jQRangeSlider
 * A javascript slider selector that supports dates
 *
 * Copyright (C) Guillaume Gautreau 2012
 * Dual licensed under the MIT or GPL Version 2 licenses.
 *
 */

(function($, undefined){
	"use strict";

	$.widget("ui.rangeSliderBar", $.ui.rangeSliderDraggable, {
		options: {
			leftHandle: null,
			rightHandle: null,
			bounds: {min: 0, max: 100},
			type: "rangeSliderHandle",
			range: false,
			drag: function() {},
			stop: function() {},
			values: {min: 0, max:20},
			wheelSpeed: 4,
			wheelMode: null
		},

		_values: {min: 0, max: 20},
		_waitingToInit: 2,
		_wheelTimeout: false,

		_create: function(){
			$.ui.rangeSliderDraggable.prototype._create.apply(this);

			this.element
				.css("position", "absolute")
				.css("top", 0)
				.addClass("ui-rangeSlider-bar");

			this.options.leftHandle
				.bind("initialize", $.proxy(this._onInitialized, this))
				.bind("mousestart", $.proxy(this._cache, this))
				.bind("stop", $.proxy(this._onHandleStop, this));

			this.options.rightHandle
				.bind("initialize", $.proxy(this._onInitialized, this))
				.bind("mousestart", $.proxy(this._cache, this))
				.bind("stop", $.proxy(this._onHandleStop, this));

			this._bindHandles();

			this._values = this.options.values;
			this._setWheelModeOption(this.options.wheelMode);
		},

		destroy: function(){
			this.options.leftHandle.unbind(".bar");
			this.options.rightHandle.unbind(".bar");
			this.options = null;

			$.ui.rangeSliderDraggable.prototype.destroy.apply(this);
		},

		_setOption: function(key, value){
			if (key === "range"){
				this._setRangeOption(value);
			} else if (key === "wheelSpeed"){
				this._setWheelSpeedOption(value);
			} else if (key === "wheelMode"){
				this._setWheelModeOption(value);
			}
		},

		_setRangeOption: function(value){
			if (typeof value !== "object" || value === null){
				value = false;
			}

			if (value === false && this.options.range === false){
				return;
			}

			if (value !== false){
				var min = valueOrFalse(value.min, this.options.range.min),
					max = valueOrFalse(value.max, this.options.range.max);

				this.options.range = {
					min: min,
					max: max
				};
			}else{
				this.options.range = false;
			}

			this._setLeftRange();
			this._setRightRange();
		},

		_setWheelSpeedOption: function(value){
			if (typeof value === "number" && value > 0){
				this.options.wheelSpeed = value;
			}
		},

		_setWheelModeOption: function(value){
			if (value === null || value === false || value === "zoom" || value === "scroll"){
				if (this.options.wheelMode !== value){
					this.element.parent().unbind("mousewheel.bar");
				}

				this._bindMouseWheel(value);
				this.options.wheelMode = value;
			}
		},

		_bindMouseWheel: function(mode){
			if (mode === "zoom"){
				this.element.parent().bind("mousewheel.bar", $.proxy(this._mouseWheelZoom, this));
			}else if (mode === "scroll"){
				this.element.parent().bind("mousewheel.bar", $.proxy(this._mouseWheelScroll, this));
			}
		},

		_setLeftRange: function(){
			if (this.options.range === false){
				return false;
			}

			var rightValue = this._values.max,
				leftRange = {min: false, max: false};

			if (typeof this.options.range.min !== "undefined" && this.options.range.min !== false){
				leftRange.max = this._leftHandle("substract", rightValue, this.options.range.min);
			}else{
				leftRange.max = false;
			}

			if (typeof this.options.range.max !== "undefined" && this.options.range.max !== false){
				leftRange.min = this._leftHandle("substract", rightValue, this.options.range.max);
			}else{
				leftRange.min = false;
			}

			this._leftHandle("option", "range", leftRange);
		},

		_setRightRange: function(){
			var leftValue = this._values.min,
				rightRange = {min: false, max:false};

			if (typeof this.options.range.min !== "undefined" && this.options.range.min !== false){
				rightRange.min = this._rightHandle("add", leftValue, this.options.range.min);
			}else {
				rightRange.min = false;
			}

			if (typeof this.options.range.max !== "undefined" && this.options.range.max !== false){
				rightRange.max = this._rightHandle("add", leftValue, this.options.range.max);
			}else{
				rightRange.max = false;
			}

			this._rightHandle("option", "range", rightRange);
		},

		_deactivateRange: function(){
			this._leftHandle("option", "range", false);
			this._rightHandle("option", "range", false);
		},

		_reactivateRange: function(){
			this._setRangeOption(this.options.range);
		},

		_onInitialized: function(){
			this._waitingToInit--;

			if (this._waitingToInit === 0){
				this._initMe();
			}
		},

		_initMe: function(){
			this._cache();
			this.min(this._values.min);
			this.max(this._values.max);

			var left = this._leftHandle("position"),
				right = this._rightHandle("position") + this.options.rightHandle.width();

			this.element.offset({
				left: left
			});

			this.element.css("width", right - left);
		},

		_leftHandle: function(){
			return this._handleProxy(this.options.leftHandle, arguments);
		},

		_rightHandle: function(){
			return this._handleProxy(this.options.rightHandle, arguments);
		},

		_handleProxy: function(element, args){
			var array = Array.prototype.slice.call(args);

			return element[this.options.type].apply(element, array);
		},

		/*
		 * Draggable
		 */

		_cache: function(){
			$.ui.rangeSliderDraggable.prototype._cache.apply(this);

			this._cacheHandles();
		},

		_cacheHandles: function(){
			this.cache.rightHandle = {};
			this.cache.rightHandle.width = this.options.rightHandle.width();
			this.cache.rightHandle.offset = this.options.rightHandle.offset();

			this.cache.leftHandle = {};
			this.cache.leftHandle.offset = this.options.leftHandle.offset();
		},

		_mouseStart: function(event){
			$.ui.rangeSliderDraggable.prototype._mouseStart.apply(this, [event]);

			this._deactivateRange();
		},

		_mouseStop: function(event){
			$.ui.rangeSliderDraggable.prototype._mouseStop.apply(this, [event]);

			this._cacheHandles();

			this._values.min = this._leftHandle("value");
			this._values.max = this._rightHandle("value");
			this._reactivateRange();

			this._leftHandle().trigger("stop");
			this._rightHandle().trigger("stop");
		},

		/*
		 * Event binding
		 */

		_onDragLeftHandle: function(event, ui){
			this._cacheIfNecessary();

			if (ui.element[0] !== this.options.leftHandle[0]){
				return;
			}

			if (this._switchedValues()){
				this._switchHandles();
				this._onDragRightHandle(event, ui);
				return;
			}

			this._values.min = ui.value;
			this.cache.offset.left = ui.offset.left;
			this.cache.leftHandle.offset = ui.offset;

			this._positionBar();
		},

		_onDragRightHandle: function(event, ui){
			this._cacheIfNecessary();

			if (ui.element[0] !== this.options.rightHandle[0]){
				return;
			}

			if (this._switchedValues()){
				this._switchHandles();
				this._onDragLeftHandle(event, ui);
				return;
			}

			this._values.max = ui.value;
			this.cache.rightHandle.offset = ui.offset;

			this._positionBar();
		},

		_positionBar: function(){
			var width = this.cache.rightHandle.offset.left + this.cache.rightHandle.width - this.cache.leftHandle.offset.left;
			this.cache.width.inner = width;

			this.element
				.css("width", width)
				.offset({left: this.cache.leftHandle.offset.left});
		},

		_onHandleStop: function(){
			this._setLeftRange();
			this._setRightRange();
		},

		_switchedValues: function(){
			if (this.min() > this.max()){
				var temp = this._values.min;
				this._values.min = this._values.max;
				this._values.max = temp;

				return true;
			}

			return false;
		},

		_switchHandles: function(){
			var temp = this.options.leftHandle;

			this.options.leftHandle = this.options.rightHandle;
			this.options.rightHandle = temp;

			this._leftHandle("option", "isLeft", true);
			this._rightHandle("option", "isLeft", false);

			this._bindHandles();
			this._cacheHandles();
		},

		_bindHandles: function(){
			this.options.leftHandle
				.unbind(".bar")
				.bind("sliderDrag.bar update.bar moving.bar", $.proxy(this._onDragLeftHandle, this));

			this.options.rightHandle
				.unbind(".bar")
				.bind("sliderDrag.bar update.bar moving.bar", $.proxy(this._onDragRightHandle, this));
		},

		_constraintPosition: function(left){
			var position = {},
				right;

			position.left = $.ui.rangeSliderDraggable.prototype._constraintPosition.apply(this, [left]);

			position.left = this._leftHandle("position", position.left);

			right = this._rightHandle("position", position.left + this.cache.width.outer - this.cache.rightHandle.width);
			position.width = right - position.left + this.cache.rightHandle.width;

			return position;
		},

		_applyPosition: function(position){
			$.ui.rangeSliderDraggable.prototype._applyPosition.apply(this, [position.left]);
			this.element.width(position.width);
		},

		/*
		 * Mouse wheel
		 */

		_mouseWheelZoom: function(event, delta, deltaX, deltaY){
			/*jshint maxstatements:17*/
			if (!this.enabled){
				return false;
			}

			var middle = this._values.min + (this._values.max - this._values.min) / 2,
				leftRange = {},
				rightRange = {};

			if (this.options.range === false || this.options.range.min === false){
				leftRange.max = middle;
				rightRange.min = middle;
			} else {
				leftRange.max = middle - this.options.range.min / 2;
				rightRange.min = middle + this.options.range.min / 2;
			}

			if (this.options.range !== false && this.options.range.max !== false){
				leftRange.min = middle - this.options.range.max / 2;
				rightRange.max = middle + this.options.range.max / 2;
			}

			this._leftHandle("option", "range", leftRange);
			this._rightHandle("option", "range", rightRange);

			clearTimeout(this._wheelTimeout);
			this._wheelTimeout = setTimeout($.proxy(this._wheelStop, this), 200);

			this.zoomIn(deltaY * this.options.wheelSpeed);

			return false;
		},

		_mouseWheelScroll: function(event, delta, deltaX, deltaY){
			if (!this.enabled){
				return false;
			}

			if (this._wheelTimeout === false){
				this.startScroll();
			} else {
				clearTimeout(this._wheelTimeout);
			}

			this._wheelTimeout = setTimeout($.proxy(this._wheelStop, this), 200);

			this.scrollLeft(deltaY * this.options.wheelSpeed);
			return false;
		},

		_wheelStop: function(){
			this.stopScroll();
			this._wheelTimeout = false;
		},

		/*
		 * Public
		 */

		min: function(value){
			return this._leftHandle("value", value);
		},

		max: function(value){
			return this._rightHandle("value", value);
		},

		startScroll: function(){
			this._deactivateRange();
		},

		stopScroll: function(){
			this._reactivateRange();
			this._triggerMouseEvent("stop");
			this._leftHandle().trigger("stop");
			this._rightHandle().trigger("stop");
		},

		scrollLeft: function(quantity){
			quantity = quantity || 1;

			if (quantity < 0){
				return this.scrollRight(-quantity);
			}

			quantity = this._leftHandle("moveLeft", quantity);
			this._rightHandle("moveLeft", quantity);

			this.update();
			this._triggerMouseEvent("scroll");
		},

		scrollRight: function(quantity){
			quantity = quantity || 1;

			if (quantity < 0){
				return this.scrollLeft(-quantity);
			}

			quantity = this._rightHandle("moveRight", quantity);
			this._leftHandle("moveRight", quantity);

			this.update();
			this._triggerMouseEvent("scroll");
		},

		zoomIn: function(quantity){
			quantity = quantity || 1;

			if (quantity < 0){
				return this.zoomOut(-quantity);
			}

			var newQuantity = this._rightHandle("moveLeft", quantity);

			if (quantity > newQuantity){
				newQuantity = newQuantity / 2;
				this._rightHandle("moveRight", newQuantity);
			}

			this._leftHandle("moveRight", newQuantity);

			this.update();
			this._triggerMouseEvent("zoom");
		},

		zoomOut: function(quantity){
			quantity = quantity || 1;

			if (quantity < 0){
				return this.zoomIn(-quantity);
			}

			var newQuantity = this._rightHandle("moveRight", quantity);

			if (quantity > newQuantity){
				newQuantity = newQuantity / 2;
				this._rightHandle("moveLeft", newQuantity);
			}

			this._leftHandle("moveLeft", newQuantity);

			this.update();
			this._triggerMouseEvent("zoom");
		},

		values: function(min, max){
			if (typeof min !== "undefined" && typeof max !== "undefined")
			{
				var minValue = Math.min(min, max),
					maxValue = Math.max(min, max);

				this._deactivateRange();
				this.options.leftHandle.unbind(".bar");
				this.options.rightHandle.unbind(".bar");

				this._values.min = this._leftHandle("value", minValue);
				this._values.max = this._rightHandle("value", maxValue);

				this._bindHandles();
				this._reactivateRange();

				this.update();
			}

			return {
				min: this._values.min,
				max: this._values.max
			};
		},

		update: function(){
			this._values.min = this.min();
			this._values.max = this.max();

			this._cache();
			this._positionBar();
		}
	});

	function valueOrFalse(value, defaultValue){
		if (typeof value === "undefined"){
			return defaultValue || false;
		}

		return value;
	}

}(jQuery));
/**
 * jQRangeSlider
 * A javascript slider selector that supports dates
 *
 * Copyright (C) Guillaume Gautreau 2012
 * Dual licensed under the MIT or GPL Version 2 licenses.
 *
 */

(function($, undefined){
	
	"use strict";

	$.widget("ui.rangeSliderLabel", $.ui.rangeSliderMouseTouch, {
		options: {
			handle: null,
			formatter: false,
			handleType: "rangeSliderHandle",
			show: "show",
			durationIn: 0,
			durationOut: 500,
			delayOut: 500,
			isLeft: false
		},

		cache: null,
		_positionner: null,
		_valueContainer:null,
		_innerElement:null,
		_value: null,

		_create: function(){
			this.options.isLeft = this._handle("option", "isLeft");

			this.element
				.addClass("ui-rangeSlider-label")
				.css("position", "absolute")
				.css("display", "block");

			this._createElements();

			this._toggleClass();

			this.options.handle
				.bind("moving.label", $.proxy(this._onMoving, this))
				.bind("update.label", $.proxy(this._onUpdate, this))
				.bind("switch.label", $.proxy(this._onSwitch, this));

			if (this.options.show !== "show"){
				this.element.hide();
			}

			this._mouseInit();
		},

		destroy: function(){
			this.options.handle.unbind(".label");
			this.options.handle = null;

			this._valueContainer = null;
			this._innerElement = null;
			this.element.empty();

			if (this._positionner) {
				this._positionner.Destroy();
				this._positionner = null;
			}

			$.ui.rangeSliderMouseTouch.prototype.destroy.apply(this);
		},

		_createElements: function(){
			this._valueContainer = $("<div class='ui-rangeSlider-label-value' />")
				.appendTo(this.element);

			this._innerElement = $("<div class='ui-rangeSlider-label-inner' />")
				.appendTo(this.element);
		},

		_handle: function(){
			var args = Array.prototype.slice.apply(arguments);

			return this.options.handle[this.options.handleType].apply(this.options.handle, args);
		},

		_setOption: function(key, value){
			if (key === "show"){
				this._updateShowOption(value);
			} else if (key === "durationIn" || key === "durationOut" || key === "delayOut"){
				this._updateDurations(key, value);
			}

			this._setFormatterOption(key, value);
		},

		_setFormatterOption: function(key, value){
			if (key === "formatter"){
				if (typeof value === "function" || value === false){
					this.options.formatter = value;
					this._display(this._value);
				}
			}
		},

		_updateShowOption: function(value){
			this.options.show = value;

			if (this.options.show !== "show"){
				this.element.hide();
				this._positionner.moving = false;
			}else{
				this.element.show();
				this._display(this.options.handle[this.options.handleType]("value"));
				this._positionner.PositionLabels();
			}
			
			this._positionner.options.show = this.options.show;
		},

		_updateDurations: function(key, value){
			if (parseInt(value, 10) !== value) return;

			this._positionner.options[key] = value;
			this.options[key] = value;
		},

		_display: function(value){
			if (this.options.formatter === false){
				this._displayText(Math.round(value));
			}else{
				this._displayText(this.options.formatter(value));
			}

			this._value = value;
		},

		_displayText: function(text){
			this._valueContainer.text(text);
		},

		_toggleClass: function(){
			this.element.toggleClass("ui-rangeSlider-leftLabel", this.options.isLeft)
				.toggleClass("ui-rangeSlider-rightLabel", !this.options.isLeft);
		},

		_positionLabels: function(){
			this._positionner.PositionLabels();
		},

		/*
		 * Mouse touch redirection
		 */
		_mouseDown: function(event){
			this.options.handle.trigger(event);
		},

		_mouseUp: function(event){
			this.options.handle.trigger(event);
		},

		_mouseMove: function(event){
			this.options.handle.trigger(event);
		},

		/*
		 * Event binding
		 */
		_onMoving: function(event, ui){
			this._display(ui.value);
		},

		_onUpdate: function(){
			if (this.options.show === "show"){
				this.update();
			}
		},

		_onSwitch: function(event, isLeft){
			this.options.isLeft = isLeft;
			
			this._toggleClass();
			this._positionLabels();
		},

		/*
		 * Label pair
		 */
		pair: function(label){
			if (this._positionner !== null) return;

			this._positionner = new LabelPositioner(this.element, label, this.widgetName, {
				show: this.options.show,
				durationIn: this.options.durationIn,
				durationOut: this.options.durationOut,
				delayOut: this.options.delayOut
			});

			label[this.widgetName]("positionner", this._positionner);
		},

		positionner: function(pos){
			if (typeof pos !== "undefined"){
				this._positionner = pos;
			}

			return this._positionner;
		},

		update: function(){
			this._positionner.cache = null;
			this._display(this._handle("value"));

			if (this.options.show === "show"){
				this._positionLabels();
			}
		}
	});

	function LabelPositioner(label1, label2, type, options){
		/*jshint maxstatements:40 */

		this.label1 = label1;
		this.label2 = label2;
		this.type = type;
		this.options = options;
		this.handle1 = this.label1[this.type]("option", "handle");
		this.handle2 = this.label2[this.type]("option", "handle");
		this.cache = null;
		this.left = label1;
		this.right = label2;
		this.moving = false;
		this.initialized = false;
		this.updating = false;

		this.Init = function(){
			this.BindHandle(this.handle1);
			this.BindHandle(this.handle2);

			if (this.options.show === "show"){
				setTimeout($.proxy(this.PositionLabels, this), 1);
				this.initialized = true;
			}else{
				setTimeout($.proxy(this.AfterInit, this), 1000);
			}

			this._resizeProxy = $.proxy(this.onWindowResize, this);

			$(window).resize(this._resizeProxy);
		}

		this.Destroy = function(){
			if (this._resizeProxy){
				$(window).unbind("resize", this._resizeProxy);
				this._resizeProxy = null;

				this.handle1.unbind(".positionner");
				this.handle1 = null;

				this.handle2.unbind(".positionner");
				this.handle2 = null;

				this.label1 = null;
				this.label2 = null;
				this.left = null;
				this.right = null;
			}
			
			this.cache = null;			
		}

		this.AfterInit = function () {
			this.initialized = true;
		}

		this.Cache = function(){
			if (this.label1.css("display") === "none"){
				return;
			}

			this.cache = {};
			this.cache.label1 = {};
			this.cache.label2 = {};
			this.cache.handle1 = {};
			this.cache.handle2 = {};
			this.cache.offsetParent = {};

			this.CacheElement(this.label1, this.cache.label1);
			this.CacheElement(this.label2, this.cache.label2);
			this.CacheElement(this.handle1, this.cache.handle1);
			this.CacheElement(this.handle2, this.cache.handle2);
			this.CacheElement(this.label1.offsetParent(), this.cache.offsetParent);
		}

		this.CacheIfNecessary = function(){
			if (this.cache === null){
				this.Cache();
			}else{
				this.CacheWidth(this.label1, this.cache.label1);
				this.CacheWidth(this.label2, this.cache.label2);
				this.CacheHeight(this.label1, this.cache.label1);
				this.CacheHeight(this.label2, this.cache.label2);
				this.CacheWidth(this.label1.offsetParent(), this.cache.offsetParent);
			}
		}

		this.CacheElement = function(label, cache){
			this.CacheWidth(label, cache);
			this.CacheHeight(label, cache);

			cache.offset = label.offset();
			cache.margin = {
				left: this.ParsePixels("marginLeft", label),
				right: this.ParsePixels("marginRight", label)
			};

			cache.border = {
				left: this.ParsePixels("borderLeftWidth", label),
				right: this.ParsePixels("borderRightWidth", label)
			};
		}

		this.CacheWidth = function(label, cache){
			cache.width = label.width();
			cache.outerWidth = label.outerWidth();
		}

		this.CacheHeight = function(label, cache){
			cache.outerHeightMargin = label.outerHeight(true);
		}

		this.ParsePixels = function(name, element){
			return parseInt(element.css(name), 10) || 0;
		}

		this.BindHandle = function(handle){
			handle.bind("updating.positionner", $.proxy(this.onHandleUpdating, this));
			handle.bind("update.positionner", $.proxy(this.onHandleUpdated, this));
			handle.bind("moving.positionner", $.proxy(this.onHandleMoving, this));
			handle.bind("stop.positionner", $.proxy(this.onHandleStop, this));
		}

		this.PositionLabels = function(){
			this.CacheIfNecessary();

			if (this.cache === null){
				return;
			}

			var label1Pos = this.GetRawPosition(this.cache.label1, this.cache.handle1),
				label2Pos = this.GetRawPosition(this.cache.label2, this.cache.handle2);

			if (this.label1[type]("option", "isLeft")){
				this.ConstraintPositions(label1Pos, label2Pos);
			}else{
				this.ConstraintPositions(label2Pos, label1Pos);
			}

			this.PositionLabel(this.label1, label1Pos.left, this.cache.label1);
			this.PositionLabel(this.label2, label2Pos.left, this.cache.label2);
		}

		this.PositionLabel = function(label, leftOffset, cache){
			var parentShift = this.cache.offsetParent.offset.left + this.cache.offsetParent.border.left,
					parentRightPosition,
					labelRightPosition,
					rightPosition;

			if ((parentShift - leftOffset) >= 0){
				label.css("right", "");
				label.offset({left: leftOffset});
			}else{
				parentRightPosition = parentShift + this.cache.offsetParent.width;
				labelRightPosition = leftOffset + cache.margin.left + cache.outerWidth + cache.margin.right;
				rightPosition = parentRightPosition - labelRightPosition;

				label.css("left", "");
				label.css("right", rightPosition);
			}
		}

		this.ConstraintPositions = function(pos1, pos2){
			if ((pos1.center < pos2.center && pos1.outerRight > pos2.outerLeft) || (pos1.center > pos2.center && pos2.outerRight > pos1.outerLeft)){
				pos1 = this.getLeftPosition(pos1, pos2);
				pos2 = this.getRightPosition(pos1, pos2);
			}
		}

		this.getLeftPosition = function(left, right){
			var center = (right.center + left.center) / 2,
				leftPos = center - left.cache.outerWidth - left.cache.margin.right + left.cache.border.left;

			left.left = leftPos;

			return left;
		}

		this.getRightPosition = function(left, right){
			var center = (right.center + left.center) / 2;

			right.left = center + right.cache.margin.left + right.cache.border.left;

			return right;
		}

		this.ShowIfNecessary = function(){
			if (this.options.show === "show" || this.moving || !this.initialized || this.updating) return;

			this.label1.stop(true, true).fadeIn(this.options.durationIn || 0);
			this.label2.stop(true, true).fadeIn(this.options.durationIn || 0);
			this.moving = true;
		}

		this.HideIfNeeded = function(){
			if (this.moving === true){
				this.label1.stop(true, true).delay(this.options.delayOut || 0).fadeOut(this.options.durationOut || 0);
				this.label2.stop(true, true).delay(this.options.delayOut || 0).fadeOut(this.options.durationOut || 0);
				this.moving = false;
			}
		}

		this.onHandleMoving = function(event, ui){
			this.ShowIfNecessary();
			this.CacheIfNecessary();
			this.UpdateHandlePosition(ui);

			this.PositionLabels();
		}

		this.onHandleUpdating = function(){
			this.updating = true;
		}

		this.onHandleUpdated = function(){
			this.updating = false;
			this.cache = null;
		}

		this.onHandleStop = function(){
			this.HideIfNeeded();
		}

		this.onWindowResize = function(){
				this.cache = null;
		}

		this.UpdateHandlePosition = function(ui){
			if (this.cache === null) return;
			
			if (ui.element[0] === this.handle1[0]){
				this.UpdatePosition(ui, this.cache.handle1);
			}else{
				this.UpdatePosition(ui, this.cache.handle2);
			}
		}

		this.UpdatePosition = function(element, cache){
			cache.offset = element.offset;
			cache.value = element.value;
		}

		this.GetRawPosition = function(labelCache, handleCache){
			var handleCenter = handleCache.offset.left + handleCache.outerWidth / 2,
				labelLeft = handleCenter - labelCache.outerWidth / 2,
				labelRight = labelLeft + labelCache.outerWidth - labelCache.border.left - labelCache.border.right,
				outerLeft = labelLeft - labelCache.margin.left - labelCache.border.left,
				top = handleCache.offset.top - labelCache.outerHeightMargin;

			return {
				left: labelLeft,
				outerLeft: outerLeft,
				top: top,
				right: labelRight,
				outerRight: outerLeft + labelCache.outerWidth + labelCache.margin.left + labelCache.margin.right,
				cache: labelCache,
				center: handleCenter
			}
		}

		this.Init();
	}

}(jQuery));


/**
 * jQRangeSlider
 * A javascript slider selector that supports dates
 *
 * Copyright (C) Guillaume Gautreau 2012
 * Dual licensed under the MIT or GPL Version 2 licenses.
 *
 */

(function ($, undefined) {
	"use strict";

	$.widget("ui.rangeSlider", {
		options: {
			bounds: {min:0, max:100},
			defaultValues: {min:20, max:50},
			wheelMode: null,
			wheelSpeed: 4,
			arrows: true,
			valueLabels: "show",
			formatter: null,
			durationIn: 0,
			durationOut: 400,
			delayOut: 200,
			range: {min: false, max: false},
			step: false,
			scales: false,
			enabled: true,
			symmetricPositionning: false
		},

		_values: null,
		_valuesChanged: false,
		_initialized: false,

		// Created elements
		bar: null,
		leftHandle: null,
		rightHandle: null,
		innerBar: null,
		container: null,
		arrows: null,
		labels: null,
		changing: {min:false, max:false},
		changed: {min:false, max:false},
		ruler: null,

		_create: function(){
			this._setDefaultValues();

			this.labels = {left: null, right:null, leftDisplayed:true, rightDisplayed:true};
			this.arrows = {left:null, right:null};
			this.changing = {min:false, max:false};
			this.changed = {min:false, max:false};

			this._createElements();

			this._bindResize();

			setTimeout($.proxy(this.resize, this), 1);
			setTimeout($.proxy(this._initValues, this), 1);
		},

		_setDefaultValues: function(){
			this._values = {
				min: this.options.defaultValues.min,
				max: this.options.defaultValues.max
			};
		},

		_bindResize: function(){
			var that = this;

			this._resizeProxy = function(e){
				that.resize(e);
			};

			$(window).resize(this._resizeProxy);
		},

		_initWidth: function(){
			this.container.css("width", this.element.width() - this.container.outerWidth(true) + this.container.width());
			this.innerBar.css("width", this.container.width() - this.innerBar.outerWidth(true) + this.innerBar.width());
		},

		_initValues: function(){
			this._initialized = true;
			this.values(this._values.min, this._values.max);
		},

		_setOption: function(key, value) {
			this._setWheelOption(key, value);		
			this._setArrowsOption(key, value);
			this._setLabelsOption(key, value);
			this._setLabelsDurations(key, value);
			this._setFormatterOption(key, value);
			this._setBoundsOption(key, value);
			this._setRangeOption(key, value);
			this._setStepOption(key, value);
			this._setScalesOption(key, value);
			this._setEnabledOption(key, value);
			this._setPositionningOption(key, value);
		},

		_validProperty: function(object, name, defaultValue){
			if (object === null || typeof object[name] === "undefined"){
				return defaultValue;
			}

			return object[name];
		},

		_setStepOption: function(key, value){
			if (key === "step"){
				this.options.step = value;
				this._leftHandle("option", "step", value);
				this._rightHandle("option", "step", value);
				this._changed(true);
			}
		},

		_setScalesOption: function(key, value){
			if (key === "scales"){
				if (value === false || value === null){
					this.options.scales = false;
					this._destroyRuler();
				}else if (value instanceof Array){
					this.options.scales = value;
					this._updateRuler();
				}
			}
		},

		_setRangeOption: function(key, value){
			if (key === "range"){
				this._bar("option", "range", value);
				this.options.range = this._bar("option", "range");
				this._changed(true);
			}
		},

		_setBoundsOption: function(key, value){
			if (key === "bounds" && typeof value.min !== "undefined" && typeof value.max !== "undefined"){
				this.bounds(value.min, value.max);
			}
		},

		_setWheelOption: function(key, value){
			if (key === "wheelMode" || key === "wheelSpeed"){
				this._bar("option", key, value);
				this.options[key] = this._bar("option", key);
			}
		},

		_setLabelsOption: function(key, value){
			if (key === "valueLabels"){
				if (value !== "hide" && value !== "show" && value !== "change"){
					return;
				}

				this.options.valueLabels = value;

				if (value !== "hide"){
					this._createLabels();
					this._leftLabel("update");
					this._rightLabel("update");
				}else{
					this._destroyLabels();
				}
			}
		},

		_setFormatterOption: function(key, value){
			if (key === "formatter" && value !== null && typeof value === "function"){
				if (this.options.valueLabels !== "hide"){
					this._leftLabel("option", "formatter", value);
					this.options.formatter = this._rightLabel("option", "formatter", value);
				}
			}
		},

		_setArrowsOption: function(key, value){
			if (key === "arrows" && (value === true || value === false) && value !== this.options.arrows){
				if (value === true){
					this.element
						.removeClass("ui-rangeSlider-noArrow")
						.addClass("ui-rangeSlider-withArrows");
					this.arrows.left.css("display", "block");
					this.arrows.right.css("display", "block");
					this.options.arrows = true;
				}else if (value === false){
					this.element
						.addClass("ui-rangeSlider-noArrow")
						.removeClass("ui-rangeSlider-withArrows");
					this.arrows.left.css("display", "none");
					this.arrows.right.css("display", "none");
					this.options.arrows = false;
				}

				this._initWidth();
			}
		},

		_setLabelsDurations: function(key, value){
			if (key === "durationIn" || key === "durationOut" || key === "delayOut"){
				if (parseInt(value, 10) !== value) return;

				if (this.labels.left !== null){
					this._leftLabel("option", key, value);
				}

				if (this.labels.right !== null){
					this._rightLabel("option", key, value);
				}

				this.options[key] = value;
			}
		},

		_setEnabledOption: function(key, value){
			if (key === "enabled"){
				this.toggle(value);
			}
		},

		_setPositionningOption: function(key, value){
			if (key === "symmetricPositionning"){
				this._rightHandle("option", key, value);
				this.options[key] = this._leftHandle("option", key, value);
			}
		},

		_createElements: function(){
			if (this.element.css("position") !== "absolute"){
				this.element.css("position", "relative");
			}

			this.element.addClass("ui-rangeSlider");

			this.container = $("<div class='ui-rangeSlider-container' />")
				.css("position", "absolute")
				.appendTo(this.element);
			
			this.innerBar = $("<div class='ui-rangeSlider-innerBar' />")
				.css("position", "absolute")
				.css("top", 0)
				.css("left", 0);

			this._createHandles();
			this._createBar();
			this.container.prepend(this.innerBar);
			this._createArrows();

			if (this.options.valueLabels !== "hide"){
				this._createLabels();
			}else{
				this._destroyLabels();
			}

			this._updateRuler();

			if (!this.options.enabled) this._toggle(this.options.enabled);
		},

		_createHandle: function(options){
			return $("<div />")[this._handleType()](options)
				.bind("sliderDrag", $.proxy(this._changing, this))
				.bind("stop", $.proxy(this._changed, this));
		},

		_createHandles: function(){
			this.leftHandle = this._createHandle({
					isLeft: true,
					bounds: this.options.bounds,
					value: this._values.min,
					step: this.options.step,
					symmetricPositionning: this.options.symmetricPositionning
			}).appendTo(this.container);
	
			this.rightHandle = this._createHandle({
				isLeft: false,
				bounds: this.options.bounds,
				value: this._values.max,
				step: this.options.step,
				symmetricPositionning: this.options.symmetricPositionning
			}).appendTo(this.container);
		},
		
		_createBar: function(){
			this.bar = $("<div />")
				.prependTo(this.container)
				.bind("sliderDrag scroll zoom", $.proxy(this._changing, this))
				.bind("stop", $.proxy(this._changed, this));
			
			this._bar({
					leftHandle: this.leftHandle,
					rightHandle: this.rightHandle,
					values: {min: this._values.min, max: this._values.max},
					type: this._handleType(),
					range: this.options.range,
					wheelMode: this.options.wheelMode,
					wheelSpeed: this.options.wheelSpeed
				});

			this.options.range = this._bar("option", "range");
			this.options.wheelMode = this._bar("option", "wheelMode");
			this.options.wheelSpeed = this._bar("option", "wheelSpeed");
		},

		_createArrows: function(){
			this.arrows.left = this._createArrow("left");
			this.arrows.right = this._createArrow("right");

			if (!this.options.arrows){
				this.arrows.left.css("display", "none");
				this.arrows.right.css("display", "none");
				this.element.addClass("ui-rangeSlider-noArrow");
			}else{
				this.element.addClass("ui-rangeSlider-withArrows");
			}
		},

		_createArrow: function(whichOne){
			var arrow = $("<div class='ui-rangeSlider-arrow' />")
				.append("<div class='ui-rangeSlider-arrow-inner' />")
				.addClass("ui-rangeSlider-" + whichOne + "Arrow")
				.css("position", "absolute")
				.css(whichOne, 0)
				.appendTo(this.element),
				target;

			if (whichOne === "right"){
				target = $.proxy(this._scrollRightClick, this);
			}else{
				target = $.proxy(this._scrollLeftClick, this);
			}

			arrow.bind("mousedown touchstart", target);

			return arrow;
		},

		_proxy: function(element, type, args){
			var array = Array.prototype.slice.call(args);

			if (element && element[type]){
				return element[type].apply(element, array);	
			}

			return null;
		},

		_handleType: function(){
			return "rangeSliderHandle";
		},

		_barType: function(){
			return "rangeSliderBar";
		},

		_bar: function(){
			return this._proxy(this.bar, this._barType(), arguments);
		},

		_labelType: function(){
			return "rangeSliderLabel";
		},

		_leftLabel: function(){
			return this._proxy(this.labels.left, this._labelType(), arguments);
		},

		_rightLabel: function(){
			return this._proxy(this.labels.right, this._labelType(), arguments);
		},

		_leftHandle: function(){
			return this._proxy(this.leftHandle, this._handleType(), arguments);
		},

		_rightHandle: function(){
			return this._proxy(this.rightHandle, this._handleType(), arguments);
		},

		_getValue: function(position, handle){
			if (handle === this.rightHandle){	
				position = position - handle.outerWidth();
			}
			
			return position * (this.options.bounds.max - this.options.bounds.min) / (this.container.innerWidth() - handle.outerWidth(true)) + this.options.bounds.min;
		},

		_trigger: function(eventName){
			var that = this;

			setTimeout(function(){
				that.element.trigger(eventName, {
						label: that.element,
						values: that.values()
					});
			}, 1);
		},

		_changing: function(){
			if(this._updateValues()){
				this._trigger("valuesChanging");
				this._valuesChanged = true;
			}
		},

		_deactivateLabels: function(){
			if (this.options.valueLabels === "change"){
				this._leftLabel("option", "show", "hide");
				this._rightLabel("option", "show", "hide");
			}
		},

		_reactivateLabels: function(){
			if (this.options.valueLabels === "change"){
				this._leftLabel("option", "show", "change");
				this._rightLabel("option", "show", "change");
			}
		},

		_changed: function(isAutomatic){
			if (isAutomatic === true){
				this._deactivateLabels();
			}

			if (this._updateValues() || this._valuesChanged){
				this._trigger("valuesChanged");

				if (isAutomatic !== true){
					this._trigger("userValuesChanged");					
				}

				this._valuesChanged = false;
			}

			if (isAutomatic === true){
				this._reactivateLabels();
			}
		},

		_updateValues: function(){
			var left = this._leftHandle("value"),
				right = this._rightHandle("value"),
				min = this._min(left, right),
				max = this._max(left, right),
				changing = (min !== this._values.min || max !== this._values.max);

			this._values.min = this._min(left, right);
			this._values.max = this._max(left, right);

			return changing;
		},

		_min: function(value1, value2){
			return Math.min(value1, value2);
		},

		_max: function(value1, value2){
			return Math.max(value1, value2);
		},

		/*
		 * Value labels
		 */
		_createLabel: function(label, handle){
			var params;

			if (label === null){
				params = this._getLabelConstructorParameters(label, handle);
				label = $("<div />")
					.appendTo(this.element)[this._labelType()](params);
			}else{
				params = this._getLabelRefreshParameters(label, handle);

				label[this._labelType()](params);
			}

			return label;
		},

		_getLabelConstructorParameters: function(label, handle){
			return {
				handle: handle,
				handleType: this._handleType(),
				formatter: this._getFormatter(),
				show: this.options.valueLabels,
				durationIn: this.options.durationIn,
				durationOut: this.options.durationOut,
				delayOut: this.options.delayOut
			};
		},

		_getLabelRefreshParameters: function(){
			return {
				formatter: this._getFormatter(),
				show: this.options.valueLabels,
				durationIn: this.options.durationIn,
				durationOut: this.options.durationOut,
				delayOut: this.options.delayOut
			};
		},

		_getFormatter: function(){
			if (this.options.formatter === false || this.options.formatter === null){
				return this._defaultFormatter;
			}

			return this.options.formatter;
		},

		_defaultFormatter: function(value){
			return Math.round(value);
		},

		_destroyLabel: function(label){
			if (label !== null){
				label[this._labelType()]("destroy");
				label.remove();
				label = null;
			}

			return label;
		},

		_createLabels: function(){
			this.labels.left = this._createLabel(this.labels.left, this.leftHandle);
			this.labels.right = this._createLabel(this.labels.right, this.rightHandle);

			this._leftLabel("pair", this.labels.right);
		},

		_destroyLabels: function(){
			this.labels.left = this._destroyLabel(this.labels.left);
			this.labels.right = this._destroyLabel(this.labels.right);
		},

		/*
		 * Scrolling
		 */
		_stepRatio: function(){
			return this._leftHandle("stepRatio");
		},

		_scrollRightClick: function(e){
			if (!this.options.enabled) return false;

			e.preventDefault();
			this._bar("startScroll");
			this._bindStopScroll();

			this._continueScrolling("scrollRight", 4 * this._stepRatio(), 1);
		},

		_continueScrolling: function(action, timeout, quantity, timesBeforeSpeedingUp){
			if (!this.options.enabled) return false;

			this._bar(action, quantity);
			timesBeforeSpeedingUp = timesBeforeSpeedingUp || 5;
			timesBeforeSpeedingUp--;

			var that = this,
				minTimeout = 16,
				maxQuantity = Math.max(1, 4 / this._stepRatio());

			this._scrollTimeout = setTimeout(function(){
				if (timesBeforeSpeedingUp === 0){
					if (timeout > minTimeout){
						timeout = Math.max(minTimeout, timeout / 1.5);	
					} else {
						quantity = Math.min(maxQuantity, quantity * 2);
					}
					
					timesBeforeSpeedingUp = 5;
				}

				that._continueScrolling(action, timeout, quantity, timesBeforeSpeedingUp);
			}, timeout);
		},

		_scrollLeftClick: function(e){
			if (!this.options.enabled) return false;

			e.preventDefault();

			this._bar("startScroll");
			this._bindStopScroll();

			this._continueScrolling("scrollLeft", 4 * this._stepRatio(), 1);
		},

		_bindStopScroll: function(){
			var that = this;
			this._stopScrollHandle = function(e){
				e.preventDefault();
				that._stopScroll();
			};

			$(document).bind("mouseup touchend", this._stopScrollHandle);
		},

		_stopScroll: function(){
			$(document).unbind("mouseup touchend", this._stopScrollHandle);
			this._stopScrollHandle = null;
			this._bar("stopScroll");
			clearTimeout(this._scrollTimeout);
		},

		/*
		 * Ruler
		 */
		_createRuler: function(){
			this.ruler = $("<div class='ui-rangeSlider-ruler' />").appendTo(this.innerBar);
		},

		_setRulerParameters: function(){
			this.ruler.ruler({
				min: this.options.bounds.min,
				max: this.options.bounds.max,
				scales: this.options.scales
			});
		},

		_destroyRuler: function(){
			if (this.ruler !== null && $.fn.ruler){
				this.ruler.ruler("destroy");
				this.ruler.remove();
				this.ruler = null;
			}
		},

		_updateRuler: function(){
			this._destroyRuler();

			if (this.options.scales === false || !$.fn.ruler){
				return;
			}

			this._createRuler();
			this._setRulerParameters();			
		},

		/*
		 * Public methods
		 */
		values: function(min, max){
			var val;

			if (typeof min !== "undefined" && typeof max !== "undefined"){
				if (!this._initialized){
					this._values.min = min;
					this._values.max = max;
					return this._values;
				}

				this._deactivateLabels();
				val = this._bar("values", min, max);
				this._changed(true);
				this._reactivateLabels();
			}else{
				val = this._bar("values", min, max);
			}

			return val;
		},

		min: function(min){
			this._values.min = this.values(min, this._values.max).min;

			return this._values.min;
		},

		max: function(max){
			this._values.max = this.values(this._values.min, max).max;

			return this._values.max;
		},
		
		bounds: function(min, max){
			if (this._isValidValue(min) && this._isValidValue(max) && min < max){
				
				this._setBounds(min, max);
				this._updateRuler();
				this._changed(true);
			}
			
			return this.options.bounds;
		},

		_isValidValue: function(value){
			return typeof value !== "undefined" && parseFloat(value) === value;
		},

		_setBounds: function(min, max){
			this.options.bounds = {min: min, max: max};
			this._leftHandle("option", "bounds", this.options.bounds);
			this._rightHandle("option", "bounds", this.options.bounds);
			this._bar("option", "bounds", this.options.bounds);
		},

		zoomIn: function(quantity){
			this._bar("zoomIn", quantity)
		},

		zoomOut: function(quantity){
			this._bar("zoomOut", quantity);
		},

		scrollLeft: function(quantity){
			this._bar("startScroll");
			this._bar("scrollLeft", quantity);
			this._bar("stopScroll");
		},

		scrollRight: function(quantity){
			this._bar("startScroll");
			this._bar("scrollRight", quantity);
			this._bar("stopScroll");
		},
		
		/**
		 * Resize
		 */
		resize: function(){
			this._initWidth();
			this._leftHandle("update");
			this._rightHandle("update");
			this._bar("update");
		},

		/*
		 * Enable / disable
		 */
		enable: function(){
			this.toggle(true);
		},

		disable: function(){
			this.toggle(false);
		},

		toggle: function(enabled){
			if (enabled === undefined) enabled = !this.options.enabled;

			if (this.options.enabled !== enabled){
				this._toggle(enabled);
			}
		},

		_toggle: function(enabled){
			this.options.enabled = enabled;
			this.element.toggleClass("ui-rangeSlider-disabled", !enabled);

			var action = enabled ? "enable" : "disable";

			this._bar(action);
			this._leftHandle(action);
			this._rightHandle(action);
			this._leftLabel(action);
			this._rightLabel(action);
		},

		/*
		 * Destroy
		 */
		destroy: function(){
			this.element.removeClass("ui-rangeSlider-withArrows ui-rangeSlider-noArrow ui-rangeSlider-disabled");

			this._destroyWidgets();
			this._destroyElements();
			
			this.element.removeClass("ui-rangeSlider");
			this.options = null;

			$(window).unbind("resize", this._resizeProxy);
			this._resizeProxy = null;
			this._bindResize = null;

			$.Widget.prototype.destroy.apply(this, arguments);
		},

		_destroyWidget: function(name){
			this["_" + name]("destroy");
			this[name].remove();
			this[name] = null;
		},

		_destroyWidgets: function(){
			this._destroyWidget("bar");
			this._destroyWidget("leftHandle");
			this._destroyWidget("rightHandle");

			this._destroyRuler();
			this._destroyLabels();
		},

		_destroyElements: function(){
			this.container.remove();
			this.container = null;

			this.innerBar.remove();
			this.innerBar = null;

			this.arrows.left.remove();
			this.arrows.right.remove();
			this.arrows = null;
		}
	});
}(jQuery));
/**
 * jQRangeSlider
 * A javascript slider selector that supports dates
 *
 * Copyright (C) Guillaume Gautreau 2012
 * Dual licensed under the MIT or GPL Version 2 licenses.
 */

(function($, undefined){
	"use strict";

	$.widget("ui.rangeSliderDraggable", $.ui.rangeSliderMouseTouch, {
		cache: null,

		options: {
			containment: null
		},

		_create: function(){
			$.ui.rangeSliderMouseTouch.prototype._create.apply(this);

			setTimeout($.proxy(this._initElementIfNotDestroyed, this), 10);
		},

		destroy: function(){
			this.cache = null;
			
			$.ui.rangeSliderMouseTouch.prototype.destroy.apply(this);
		},

		_initElementIfNotDestroyed: function(){
			if (this._mouseInit){
				this._initElement();
			}
		},

		_initElement: function(){
			this._mouseInit();
			this._cache();
		},

		_setOption: function(key, value){
			if (key === "containment"){
				if (value === null || $(value).length === 0){
					this.options.containment = null
				}else{
					this.options.containment = $(value);
				}
			}
		},

		/*
		 * UI mouse widget
		 */

		_mouseStart: function(event){
			this._cache();
			this.cache.click = {
					left: event.pageX,
					top: event.pageY
			};

			this.cache.initialOffset = this.element.offset();

			this._triggerMouseEvent("mousestart");

			return true;
		},

		_mouseDrag: function(event){
			var position = event.pageX - this.cache.click.left;

			position = this._constraintPosition(position + this.cache.initialOffset.left);

			this._applyPosition(position);

			this._triggerMouseEvent("sliderDrag");

			return false;
		},

		_mouseStop: function(){
			this._triggerMouseEvent("stop");
		},

		/*
		 * To be overriden
		 */

		_constraintPosition: function(position){
			if (this.element.parent().length !== 0 && this.cache.parent.offset !== null){
				position = Math.min(position, 
					this.cache.parent.offset.left + this.cache.parent.width - this.cache.width.outer);
				position = Math.max(position, this.cache.parent.offset.left);
			}

			return position;
		},

		_applyPosition: function(position){
			var offset = {
				top: this.cache.offset.top,
				left: position
			}

			this.element.offset({left:position});

			this.cache.offset = offset;
		},

		/*
		 * Private utils
		 */

		_cacheIfNecessary: function(){
			if (this.cache === null){
				this._cache();
			}
		},

		_cache: function(){
			this.cache = {};

			this._cacheMargins();
			this._cacheParent();
			this._cacheDimensions();

			this.cache.offset = this.element.offset();
		},

		_cacheMargins: function(){
			this.cache.margin = {
				left: this._parsePixels(this.element, "marginLeft"),
				right: this._parsePixels(this.element, "marginRight"),
				top: this._parsePixels(this.element, "marginTop"),
				bottom: this._parsePixels(this.element, "marginBottom")
			};
		},

		_cacheParent: function(){
			if (this.options.parent !== null){
				var container = this.element.parent();

				this.cache.parent = {
					offset: container.offset(),
					width: container.width()
				}
			}else{
				this.cache.parent = null;
			}
		},

		_cacheDimensions: function(){
			this.cache.width = {
				outer: this.element.outerWidth(),
				inner: this.element.width()
			}
		},

		_parsePixels: function(element, string){
			return parseInt(element.css(string), 10) || 0;
		},

		_triggerMouseEvent: function(event){
			var data = this._prepareEventData();

			this.element.trigger(event, data);
		},

		_prepareEventData: function(){
			return {
				element: this.element,
				offset: this.cache.offset || null
			};
		}
	});

}(jQuery));