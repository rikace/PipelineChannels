namespace ChannelsPipeline

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open FSharp.Control.Tasks.ContextInsensitive
open SixLabors.ImageSharp
open SixLabors.ImageSharp.PixelFormats
open SixLabors.ImageSharp.Processing


module ChannelsExt =

    let castTask (task: Task<unit>) = task :> Task

    let forEachAsync (f: 'a -> Task<unit>) (cTok: CancellationToken) (asyncEn: IAsyncEnumerable<'a>) =
        task {
            let mutable canMoveNext = true
            let enumerator = asyncEn.GetAsyncEnumerator(cTok)
            while canMoveNext do
                let! next = enumerator.MoveNextAsync()
                canMoveNext <- next
                if canMoveNext then
                    do! f enumerator.Current } :> Task

    let broadcast (cTok: CancellationToken) n (channel: ChannelReader<'a>) =
        let outputs = Array.init n (fun i -> Channel.CreateUnbounded<'a>())
        let _ = Task.Run(Func<Task>(fun () -> castTask <| task {
            do!
                channel.ReadAllAsync(cTok)
                |> forEachAsync (fun item -> task {
                    for output in outputs do
                        do! output.Writer.WriteAsync(item, cTok)
                    }) cTok
            for output in outputs do output.Writer.Complete()
        }))
        outputs |> Array.map(fun ch -> ch.Reader)


    let join (cTok: CancellationToken) (inputs: ChannelReader<'a> array) =
        let output = Channel.CreateUnbounded<'a>()
        let writeAsync (input: ChannelReader<'a>) =
            task {
                do!
                    input.ReadAllAsync(cTok)
                    |> forEachAsync (fun item -> task {
                         do! output.Writer.WriteAsync(item)
                       }) cTok
            } :> Task
        let _ = Task.Run(Func<Task>(fun () -> castTask <| task {
            do!
                inputs
                |> Array.map writeAsync
                |> Task.WhenAll
            output.Writer.Complete()
            }))
        output.Reader

    let pipeline (cTok: CancellationToken) (projection: 'a -> Task<'b>) (reader: ChannelReader<'a>) =
        let pipe = Channel.CreateBounded<'b>(10)
        let writer = pipe.Writer
        let _ = Task.Run(Func<Task>(fun () -> castTask <| task {
                do!
                    reader.ReadAllAsync(cTok)
                     |> forEachAsync (fun item -> task {
                          let! outputItem = projection item
                          do! writer.WriteAsync(outputItem)
                        }) cTok
                writer.Complete()
            }
          )
        )
        pipe.Reader

    let pipelineAction (cTok: CancellationToken) (action: 'a -> Task) (reader: ChannelReader<'a>) =
        Task.Run(Func<Task>(fun () -> castTask <| task {
                do!
                    reader.ReadAllAsync(cTok)
                     |> forEachAsync (fun item -> task {
                          do! action item
                        }) cTok
            }
          )
        )

    module ProducerConsumer =

       let forEachAsync (f: 'a -> Task<unit>) (asyncEn: IAsyncEnumerable<'a>) =
            task {
                let mutable canMoveNext = true
                let enumerator = asyncEn.GetAsyncEnumerator()
                while canMoveNext do
                    let! next = enumerator.MoveNextAsync()
                    canMoveNext <- next
                    if canMoveNext then
                        do! f enumerator.Current } :> Task

       let channel = Channel.CreateUnbounded<int>()

       let producer () = task {
            for item in [0..100] do
                do! channel.Writer.WriteAsync(item)
            channel.Writer.Complete()
        }

       let consumer () = task {
            do!
              channel.Reader.ReadAllAsync()
              |> forEachAsync (fun item -> task {
                   printfn $"Received item %d{item}"
              })
       }

       let runProducerConsumer () =
         let producerTask =  Task.Run(Func<Task>(fun () -> castTask <| producer() ))
         let consumerTask =  Task.Run(Func<Task>(fun () -> castTask <| consumer() ))
         Task.WhenAll([producerTask; consumerTask])

module ImageProcessingHelpers =

    type ImageInfo = {
        name: string
        source: string
        destination: string
        image: Image<Rgba32>
    }

    let loadImageAsync (filePath: string) : Task<Image<Rgba32>> = task {
        printfn $"Loading Image %s{Path.GetFileName(filePath)}..."
        use sourceStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 0x1000, true)
        let result = Array.zeroCreate<byte> (int sourceStream.Length)
        let! count = sourceStream.ReadAsync(result, 0, int sourceStream.Length)
        let imageArray = Array.sub result 0 count
        return Image.Load<Rgba32>(imageArray)
    }

    let resize(source: Image<Rgba32>) (newWidth: int) (newHeight: int) = task {
        let image = source.Clone()
        image.Mutate(Action<_>(fun (x: IImageProcessingContext) -> x.Resize(newWidth, newHeight) |> ignore))
        return image
    }

    let apply3DEffect (source: Image<Rgba32>) = task {
        let image = source.Clone()
        let w = image.Width
        let h = image.Height
        for x = 20 to w - 1 do
            for y = 0 to h - 1 do
                let c1 = image.[x, y]
                let c2 = image.[x - 20, y]
                image.[x - 20, y] <- new Rgba32(c1.R, c2.G, c2.B)
        return image
    }

    let scaleImage (imageInfo: ImageInfo) = task {
        printfn $"Scaling Image %s{Path.GetFileName(imageInfo.name)}..."
        let size = 200
        let! resizedImage = resize imageInfo.image size size
        return { imageInfo with
                    name = sprintf $"scaled%d{size}_{imageInfo.name}"
                    image = resizedImage }
    }

    let convertTo3D (imageInfo: ImageInfo) = task {
        printfn $"Converting to 3D Image %s{Path.GetFileName(imageInfo.name)}..."
        let! converted3DImage = apply3DEffect(imageInfo.image)
        return { imageInfo with
                    name = sprintf $"3DEffect_{imageInfo.name}"
                    image = converted3DImage }
    }

    type ImageFilter =
        | Red
        | Green
        | Blue
        | Gray

    let setFilter(imageInfo: ImageInfo) (imageFilter: ImageFilter) = task {
        let image = imageInfo.image.Clone();
        let w = image.Width;
        let h = image.Height;
        for x = 0 to w - 1 do
            for y = 0 to h - 1 do
                match imageFilter with
                | Red -> image.[x, y] <- Rgba32(image.[x, y].R, 0uy, 0uy)
                | Green -> image.[x, y] <- Rgba32(0uy, image.[x, y].G, 0uy)
                | Blue -> image.[x, y] <- Rgba32(0uy, 0uy, image.[x, y].B)
                | Gray ->
                    let gray = ((0.299 * float image.[x, y].R) + (0.587 * float image.[x, y].G) + (0.114 * float image.[x, y].B)) |> byte
                    image.[x, y] <- Rgba32(gray, gray, gray)
        return { imageInfo with
                    name = sprintf $"filter%A{imageFilter}_{imageInfo.name}"
                    image = image }
    }

    let saveImage (imageInfo: ImageInfo) =
        task {
            let filePathDestination = Path.Combine(imageInfo.destination, imageInfo.name)
            printfn $"Saving Image %s{imageInfo.name}..."
            do! imageInfo.image.SaveAsync(filePathDestination)
            imageInfo.image.Dispose()
        } :> Task

module ChannelPipeline =
    open ChannelsExt
    open ImageProcessingHelpers

    let loadImage destination (reader: ChannelReader<string>) (writer: ChannelWriter<ImageInfo>) (cTok: CancellationToken) =
        printfn $"Stage 1 - is running with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
        task {
                do!
                    reader.ReadAllAsync(cTok)
                     |> forEachAsync (fun path -> task {
                          let! image = ImageProcessingHelpers.loadImageAsync path
                          let outputItem =
                            {
                                name = Path.GetFileName(path)
                                source = path
                                destination = destination
                                image = image
                            }
                          do! writer.WriteAsync(outputItem)
                          printfn $"Stage 1 - add data with Thread ID #${Thread.CurrentThread.ManagedThreadId}"
                        }) cTok
                writer.Complete()
            } :> Task

    let scaleImage (reader: ChannelReader<ImageInfo>) (writer: ChannelWriter<ImageInfo>) (cTok: CancellationToken) =
        printfn $"Stage 2 - is running with Thread ID #s{Thread.CurrentThread.ManagedThreadId}"
        castTask <| task {
                do!
                    reader.ReadAllAsync(cTok)
                     |> forEachAsync (fun imageInfo -> task {
                          let! outputItem = ImageProcessingHelpers.scaleImage imageInfo
                          do! writer.WriteAsync(outputItem)
                          printfn $"Stage 2 - add data with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
                        }) cTok
                writer.Complete()
            }
        // stage3Data.Writer.Complete();

    let convertTo3D (reader: ChannelReader<ImageInfo>) (writer: ChannelWriter<ImageInfo>) (cTok: CancellationToken) =
        printfn $"Stage 3 - is running with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
        castTask <| task {
                do!
                    reader.ReadAllAsync(cTok)
                     |> forEachAsync (fun imageInfo -> task {
                          let! outputItem = ImageProcessingHelpers.convertTo3D imageInfo
                          do! writer.WriteAsync(outputItem)
                          printfn $"Stage 3 - add data with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
                         }) cTok
                writer.Complete()
            }
        //  stage4Data.Writer.Complete();

    let setFilter filter (reader: ChannelReader<ImageInfo>) (writer: ChannelWriter<ImageInfo>) (cTok: CancellationToken) =
        printfn $"Stage 4 - is running with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
        castTask <| task {
                do!
                    reader.ReadAllAsync(cTok)
                     |> forEachAsync (fun imageInfo -> task {
                          let! outputItem = ImageProcessingHelpers.setFilter imageInfo filter // ImageFilters.Green
                          do! writer.WriteAsync(outputItem)
                          printfn $"Stage 4 - add data with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
                       }) cTok
            }

    let saveImage (reader: ChannelReader<ImageInfo>) (cTok: CancellationToken) =
        printfn $"Stage 5- is running with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
        castTask <| task {
                do!
                    reader.ReadAllAsync(cTok)
                     |> forEachAsync (fun item -> task {
                          do! ImageProcessingHelpers.saveImage item
                          printfn $"Stage 5 - save data with Thread ID #%d{Thread.CurrentThread.ManagedThreadId}"
                       }) cTok
            }

    let dataGenerator sourceImages (cTok: CancellationToken) : ChannelReader<string> =
        let images = Directory.GetFiles(sourceImages, "*.jpg")
        let channel = Channel.CreateBounded<string>(10)
        let rnd = Random()

        let _ = Task.Run(Func<Task>(fun () -> castTask <| task {
            do!
                images
                |> Seq.map (fun image ->
                        task {
                            do! channel.Writer.WriteAsync(image, cTok)
                            do! Task.Delay(TimeSpan.FromSeconds(float(rnd.Next(3))))
                        }
                        :> Task)
                |> Task.WhenAll
            channel.Writer.Complete()
            }), cancellationToken = cTok
        )
        channel.Reader

    let executeSequential (source: string) destination = task {

        if Directory.Exists destination |> not then
            Directory.CreateDirectory destination |> ignore

        let cTok = new CancellationTokenSource()

        let pipelineCtok f = pipeline cTok.Token f

        do!
            dataGenerator source cTok.Token
            |> pipelineCtok (fun path -> task {
                let! image = ImageProcessingHelpers.loadImageAsync path
                let outputItem =
                     {
                         name = Path.GetFileName(path)
                         source = path
                         destination = destination
                         image = image
                     }
                return outputItem
                })
            |> pipelineCtok (fun imageInfo -> ImageProcessingHelpers.scaleImage imageInfo)
            |> pipelineCtok (fun imageInfo -> ImageProcessingHelpers.convertTo3D imageInfo)
            |> pipelineAction cTok.Token (fun imageInfo -> ImageProcessingHelpers.saveImage imageInfo)

    }
    let executeForkJoin (source: string) destination = task {

        if Directory.Exists destination |> not then
            Directory.CreateDirectory destination |> ignore

        let cTok = new CancellationTokenSource()
        let pipelineCtok f = pipeline cTok.Token f

        let broadcast = broadcast cTok.Token 4 (dataGenerator source cTok.Token)

        let pipe reader =
            reader
            |> pipelineCtok (fun path -> task {
                let! image = ImageProcessingHelpers.loadImageAsync path
                let outputItem =
                     {
                         name = Path.GetFileName(path)
                         source = path
                         destination = destination
                         image = image
                     }
                return outputItem
                })
            |> pipelineCtok (fun imageInfo -> ImageProcessingHelpers.scaleImage imageInfo)
            |> pipelineCtok (fun imageInfo -> ImageProcessingHelpers.convertTo3D imageInfo)

        do!
            broadcast
            |> Array.map pipe
            |> join cTok.Token
            |> pipelineAction cTok.Token (fun imageInfo -> ImageProcessingHelpers.saveImage imageInfo)

    }


    let executeMultiForkJoin (source: string) destination = task {

        if Directory.Exists destination |> not then
            Directory.CreateDirectory destination |> ignore

        let cTok = new CancellationTokenSource()
        let pipelineCtok f = pipeline cTok.Token f

        let pipe transform reader =
            reader
            |> pipelineCtok (fun path -> task {
                let! image = ImageProcessingHelpers.loadImageAsync path
                let outputItem =
                     {
                         name = Path.GetFileName(path)
                         source = path
                         destination = destination
                         image = image
                     }
                return outputItem
                })
            |> pipelineCtok (fun imageInfo -> ImageProcessingHelpers.scaleImage imageInfo)
            |> pipelineCtok (fun imageInfo -> transform imageInfo)

        let pipe3D = pipe (fun imageInfo -> ImageProcessingHelpers.convertTo3D imageInfo)
        let pipeRedFilter = pipe (fun imageInfo -> ImageProcessingHelpers.setFilter imageInfo ImageFilter.Red)
        let pipeBlueFilter = pipe (fun imageInfo -> ImageProcessingHelpers.setFilter imageInfo ImageFilter.Blue)
        let pipeGreenFilter = pipe (fun imageInfo -> ImageProcessingHelpers.setFilter imageInfo ImageFilter.Green)


        let collapse (source: ChannelReader<'a>) (maps: (ChannelReader<'a> -> ChannelReader<'b>) array) =
            let countBranches = maps |> Array.length
            let sources = broadcast cTok.Token countBranches source //
            Array.zip sources maps
            |> Array.map (fun (reader, map) -> map reader)
            |> join cTok.Token

        let dataSource = dataGenerator source cTok.Token
        do!
           collapse dataSource [|pipe3D; pipeRedFilter; pipeBlueFilter; pipeGreenFilter|]
           |> pipelineAction cTok.Token (fun imageInfo -> ImageProcessingHelpers.saveImage imageInfo)
    }
