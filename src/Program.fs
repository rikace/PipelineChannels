open System
open System.IO

open ChannelsPipeline


[<EntryPoint>]
let main argv =

    let path = "./Images"
    let files = Directory.GetFiles(path)


    (ChannelPipeline.executeMultiForkJoin path "./Output").GetAwaiter().GetResult()


    printfn $"Files count %d{files.Length}"


    Console.ReadLine() |> ignore
    0
