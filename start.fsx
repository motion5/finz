//currency;Date;Open;High;Low;Close;Volume;Market Cap
open System
open System.IO

type CoinData = {
   Currency: String;
   Date: DateTime;
   Open: decimal;
   High: decimal;
   Low: decimal;
   Close: decimal;
   Volume: int;
   MarketCap: int;
}

let csvReader fileName =  
  async {
    try
      use fileStream = File.Open(fileName, FileMode.Open)
      use streamReader = new StreamReader(fileStream)
      let! result = streamReader.ReadToEndAsync() |> Async.AwaitTask
      return Some result
    with
    | _ -> return None
  }

let splitByLine (str: string) =
  str.Split([| '\n' |], StringSplitOptions.RemoveEmptyEntries) |> Seq.toList

let monthNameNumber =
  [
    ("Jan", "1")
    ("Feb", "2")
    ("Mar", "3")
    ("Apr", "4")
    ("May", "5")
    ("Jun", "6")
    ("Jul", "7")
    ("Aug", "8")
    ("Sep", "9")
    ("Oct", "10")
    ("Nov", "11")
    ("Dec", "12")
  ] |> Map.ofList

type Result<'T, 'E> =
  | Success of 'T
  | Error of 'E

let parseDecimal (str: string) =
  if String.IsNullOrWhiteSpace(str) then 0m
  else 
    try 
      str |> decimal
    with 
    | _ ->
      printfn "cant parse %s" str
      0m


let parseDate (str: string) =
  let splitDate = str.Trim( '"' ).Split([|' '|])
  DateTime.Parse(splitDate.[1] + "/" + monthNameNumber.[splitDate.[0]] + "/" + splitDate.[2])
  

let getRecord (line: string) =
  let values = line.Split([| ',' |])
  //printfn "%A" values.[8]
  //printfn "%A" values.[9]
  //printfn "%A" values.[10]
  let openV = values.[3] |> parseDecimal
  let high = values.[4] |> parseDecimal
  let low = values.[5] |> parseDecimal
  let close = values.[6] |> parseDecimal

  if openV = 0m then printfn "Unable to parse open :: %s" line else () |> ignore
  if high = 0m then printfn "Unable to parse high :: %s" line else () |> ignore
  if low = 0m then printfn "Unable to parse low :: %s" line else () |> ignore
  if close = 0m then printfn "Unable to parse close :: %s" line else () |> ignore

  {
    Currency = values.[0]
    Date = parseDate(values.[1] + values.[2])
    Open = values.[3] |> parseDecimal
    High = values.[4] |> parseDecimal
    Low = values.[5] |> parseDecimal
    Close = values.[6] |> parseDecimal
    Volume = 0
    MarketCap = 0
  }

let readCsv = 
  async {
    let! res = "./data/consolidated_coin_data.csv" |> csvReader
    return res 
      |> Option.map ( fun data ->
        data
        |> splitByLine
        |> Seq.skip 1
        |> Seq.toList
        |> List.map getRecord
    )
  }

readCsv |> Async.RunSynchronously
