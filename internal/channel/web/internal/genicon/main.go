// Command genicon generates installable PNGs from the web icon's flat design.
package main

import (
	"image"
	"image/color"
	"image/png"
	"os"
)

const (
	baseSize   = 180
	baseRadius = 28
	baseScale  = 14
)

var glyphs = []struct {
	x    int
	rows [7]string
}{
	{18, [7]string{"1111", "1", "1", "1", "1", "1", "1111"}},
	{102, [7]string{"1111", "0001", "0001", "1111", "0001", "0001", "1111"}},
}

func main() {
	for _, output := range []struct {
		name string
		size int
	}{
		{name: "apple-touch-icon.png", size: 180},
		{name: "icon-192.png", size: 192},
		{name: "icon-512.png", size: 512},
	} {
		if err := generate(output.name, output.size); err != nil {
			panic(err)
		}
	}
}

func generate(name string, size int) error {
	canvas := image.NewRGBA(image.Rect(0, 0, size, size))
	dark := color.RGBA{R: 0x0b, G: 0x0d, B: 0x10, A: 0xff}
	white := color.RGBA{R: 0xff, G: 0xff, B: 0xff, A: 0xff}
	radius := scaled(baseRadius, size)
	glyphScale := scaled(baseScale, size)
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			if insideRoundedSquare(x, y, size, radius) {
				canvas.SetRGBA(x, y, dark)
			}
		}
	}
	for _, glyph := range glyphs {
		for row, pixels := range glyph.rows {
			for column, pixel := range pixels {
				if pixel != '1' {
					continue
				}
				fill(canvas, scaled(glyph.x, size)+column*glyphScale, scaled(41, size)+row*glyphScale, glyphScale, glyphScale, white)
			}
		}
	}
	file, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := png.Encode(file, canvas); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func scaled(value, size int) int {
	return (value*size + baseSize/2) / baseSize
}

func insideRoundedSquare(x, y, size, radius int) bool {
	nearX := x
	if x >= size-radius {
		nearX = size - 1 - x
	}
	nearY := y
	if y >= size-radius {
		nearY = size - 1 - y
	}
	if nearX >= radius || nearY >= radius {
		return true
	}
	dx, dy := radius-1-nearX, radius-1-nearY
	return dx*dx+dy*dy < radius*radius
}

func fill(canvas *image.RGBA, x, y, width, height int, shade color.RGBA) {
	for row := y; row < y+height; row++ {
		for column := x; column < x+width; column++ {
			canvas.SetRGBA(column, row, shade)
		}
	}
}
