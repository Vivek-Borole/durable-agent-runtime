// demo-gif produces a small, synthetic-only animated walkthrough from the
// reviewed console screenshots. It does not contact a browser or any service.
package main

import (
	"image"
	"image/color/palette"
	"image/draw"
	"image/gif"
	_ "image/jpeg"
	"os"
)

func main() {
	paths := []string{
		"docs/evidence/screenshots/console-awaiting-approval.png",
		"docs/evidence/screenshots/console-succeeded.png",
	}
	frames := make([]*image.Paletted, 0, len(paths)*2)
	delays := make([]int, 0, len(paths)*2)
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		decoded, _, err := image.Decode(file)
		_ = file.Close()
		if err != nil {
			panic(err)
		}
		bounds := decoded.Bounds()
		frame := image.NewPaletted(bounds, palette.Plan9)
		draw.FloydSteinberg.Draw(frame, bounds, decoded, bounds.Min)
		frames = append(frames, frame)
		delays = append(delays, 35)
	}
	output, err := os.Create("docs/evidence/synthetic-demo.gif")
	if err != nil {
		panic(err)
	}
	defer output.Close()
	if err = gif.EncodeAll(output, &gif.GIF{Image: frames, Delay: delays, LoopCount: 0}); err != nil {
		panic(err)
	}
}
