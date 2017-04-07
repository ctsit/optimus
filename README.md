# optimus
Optimus transforms and rolls out your csv data to redcap.

## What is optimus? ##

Optimus is a harness and strategy for extracting data from CSV to redcap (subject, event) 
tuple json data structures.

## Why is this needed? ##

Often there is a mismatch in the way that CSV files store data to the way that redcap needs to consume
that data. I know that you are thinking that because redcap takes CSV imports that this should be simple,
but where one gets tripped up are the redcap specific pieces.

For instance, if given a CSV of lab results for many different patients how does one determine which form
that lab result should be on? If there are many labs across many dates for a single patient how should those
be translated to events? How should you deal with branching logic in forms; after all, a CSV is a flat structure 
with no logic inside it.

So we need to acknowledge that redcap projects are extremely varied and will almost always require
unique consideration. Writing a tool to handle that complexity in configuration or in a domain specific language
is not a good idea simply due to the breadth of the problem.

Futher the input CSV could contain information pretaining to more than one redcap field in a single row. This is
dependent on the input file recieved. So it looks like we are in a pickle, the input changes, the output can be 
almost anything due to redcap's configurability. How can we possibly avoid one-off solutions for every project?

Enter optimus.

## How does it all work? ##

As stated previously, optimus is more of a harness and strategy than a software tool. It takes a yaml configuration
file as which has two principle responsibilities. 

  * Determine what data a CSV row contains
  * Specify the way that forms are defined in the target redcap project

In order to explain why these have to be done, I'll give examples. 
A row could be a lab result for a subject and contained within could be the type of lab,
the subject to which it corresponds, the date the lab occured, the date the result was obtained,
and the actual value of the lab itself.

So we have the information that identifies the data (the `row_key`), 
tells you which subject it needs to be associated with,
a date which roughly corresponds to an event,
and what needs to go in some redcap field.
Since these are all the pieces of data in a row, this is the row's `outputs`.

These outputs will be passed to the `pipeline` function that is defined with the project specific module 
specified by the `project` field in the yaml configuration. 

This pipeline function will pass the data given from the CSV parsing
