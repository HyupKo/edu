// Calculate Chart colors to add colors array.
// It's colors base on default google chart color array.
$(function($){
	var colors = new Array();

    function rgbToHsl(rgb) {
		var transRgb = rgb.split("#")[1];
		var r = parseInt(transRgb.substring(0, 2), 16);
		var g = parseInt(transRgb.substring(2, 4), 16);
		var b = parseInt(transRgb.substring(4, 6), 16);
		
	    r /= 255, g /= 255, b /= 255;
	    var max = Math.max(r, g, b), min = Math.min(r, g, b);
	    var h, s, l = (max + min) / 2;
	    
	    if (max == min)
	        h = s = 0; // achromatic
	    else {
	        var d = max - min;
	        s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
	        switch(max) {
	            case r: h = (g - b) / d + (g < b ? 6 : 0); break;
	            case g: h = (b - r) / d + 2; break;
	            case b: h = (r - g) / d + 4; break;
	        }
	        h /= 6;
	    }
	    
	    return [h, s, l];
	}
	
	function hslToRgb(h, s, l) {
	    var r, g, b;
	    
	    if (s == 0)
	        r = g = b = l; // achromatic
	    else{
	        var hue2rgb = function hue2rgb(p, q, t) {
	            if (t < 0) t += 1;
	            if (t > 1) t -= 1;
	            if (t < 1/6) return p + (q - p) * 6 * t;
	            if (t < 1/2) return q;
	            if (t < 2/3) return p + (q - p) * (2/3 - t) * 6;
	            
	            return p;
	        }

	        var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
	        var p = 2 * l - q;
	        r = hue2rgb(p, q, h + 1/3);
	        g = hue2rgb(p, q, h);
	        b = hue2rgb(p, q, h - 1/3);
	    }
	    
	    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
	}
	
	function calsColors(rgb, slice) {
		var hsl = rgbToHsl(rgb);
		var baseHue = hsl[0] * 240;
		var step = (255 / slice);
		var baseRgb = hslToRgb(hsl[0], hsl[1], hsl[2]);
		colors.push("#" + baseRgb[0].toString(16) + baseRgb[1].toString(16) + baseRgb[2].toString(16));
		
		for (var i=0 ; i<slice ; i++) {
			var tempHue = (baseHue + step * i) % 240;
			var hslArray = hslToRgb(tempHue / 255, hsl[1], hsl[2]);
			colors.push("#" + hslArray[0].toString(16) + hslArray[1].toString(16) + hslArray[2].toString(16));
		}
	}
});
