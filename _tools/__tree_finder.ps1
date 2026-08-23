#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Erstellt eine baumartige Darstellung einer Verzeichnisstruktur (inkl. Dateien).
    Zeigt nur die Ebenen relativ zum angegebenen ROOT-Verzeichnis.

.USAGE
    .\__tree_finder.ps1 [ROOT] [--all] [--max-depth N] [--out FILE]

    ROOT            – Start-Verzeichnis (Standard: .)
    --all           – auch versteckte Dateien/Ordner (.*) einbeziehen
    --max-depth N   – maximale Anzahl anzuzeigender Ebenen (0 = unbegrenzt)
    --out FILE      – Ergebnis in FILE schreiben (UTF-8 ohne BOM)

    Beispiele:
        .\__tree_finder.ps1 .
        .\__tree_finder.ps1 . --all
        .\__tree_finder.ps1 . --max-depth 6
        .\__tree_finder.ps1 . --out verzeichnis.txt
        .\__tree_finder.ps1 \\192.168.0.80\data\...\src
#>

# ----------------------------------------------------------------------
# 1.  Grundkonfiguration
# ----------------------------------------------------------------------
$ErrorActionPreference = 'Stop'

# Box-Drawing-Zeichen sauber im Terminal darstellen (Windows PowerShell nutzt
# sonst gern die OEM-Codepage und zerlegt │ ├ └ ─ ). Fehler ignorieren,
# falls die Ausgabe umgeleitet ist.
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

$RootPath = '.'
$ShowAll  = $false
$MaxDepth = $null          # $null = unbegrenzt
$OutFile  = $null

function Throw-TreeError {
    param([string]$Message)
    [Console]::Error.WriteLine("ERROR: $Message")
    exit 2
}

# ----------------------------------------------------------------------
# 2.  Argument-Parsing
# ----------------------------------------------------------------------
$pos = 0
while ($pos -lt $args.Count) {
    $arg = $args[$pos]

    switch -regex ($arg) {
        '^--all$' {
            $ShowAll = $true
            $pos++
        }
        '^--max-depth$' {
            $pos++
            if ($pos -ge $args.Count) { Throw-TreeError "--max-depth benötigt ein Argument." }
            $val = $args[$pos]
            if (-not ([int]::TryParse($val, [ref]$null))) {
                Throw-TreeError "--max-depth muss eine ganze Zahl sein."
            }
            $MaxDepth = [int]$val
            $pos++
        }
        '^--out$' {
            $pos++
            if ($pos -ge $args.Count) { Throw-TreeError "--out benötigt einen Dateinamen." }
            $OutFile = $args[$pos]
            $pos++
        }
        '^--' {
            Throw-TreeError "Unbekannte Option: $arg"
        }
        default {
            $RootPath = $arg
            $pos++
        }
    }
}

# 0 (oder negativ) laut Doku = unbegrenzt -> auf $null normalisieren.
if ($null -ne $MaxDepth -and $MaxDepth -le 0) { $MaxDepth = $null }

# ----------------------------------------------------------------------
# 3.  Validierung
# ----------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $RootPath -PathType Container)) {
    Throw-TreeError "ROOT ist kein Verzeichnis: $RootPath"
}

$RootFull = (Resolve-Path -LiteralPath $RootPath).Path.TrimEnd('\', '/')

# ----------------------------------------------------------------------
# 4.  Baum-Zeichen  (kanonisches tree-Layout, jede Zelle = 4 Zeichen breit)
# ----------------------------------------------------------------------
$VERT   = [char]0x2502   # │
$BRANCH = [char]0x251C   # ├
$CORNER = [char]0x2514   # └
$HLINE  = [char]0x2500   # ─

$CONNECT_MID  = "$BRANCH$HLINE$HLINE "   # "├── "  -> Element, weitere folgen
$CONNECT_LAST = "$CORNER$HLINE$HLINE "   # "└── "  -> letztes Element im Ordner
$INDENT_MID   = "$VERT   "               # "│   "  -> Vorfahr hatte noch Geschwister
$INDENT_LAST  = "    "                   #  4 Spaces -> Vorfahr war letztes Element

$ExcludeDirs = @('.git', '.venv', '__pycache__', 'node_modules')

# Gemeinsamer Ergebnispuffer (List statt += -> O(n) statt O(n^2)).
$OutputLines = [System.Collections.Generic.List[string]]::new()

# ----------------------------------------------------------------------
# 5.  Hilfsfunktionen
# ----------------------------------------------------------------------

# Ordinale Sortierung der Geschwister nach Name.
# Entspricht exakt "LC_ALL=C sort" der funktionierenden .sh-Variante und
# vermeidet die kultur-abhängige Kollation, die im Original die Trennzeichen
# ignoriert und den Baum zerstört hat.
function Sort-SiblingsOrdinal {
    param([object[]]$Items)

    if ($null -eq $Items -or $Items.Count -le 1) { return $Items }

    $list = [System.Collections.Generic.List[object]]::new()
    $list.AddRange([object[]]$Items)

    $comparer = [System.Comparison[object]] {
        param($a, $b)
        [string]::CompareOrdinal($a.Name, $b.Name)
    }
    $list.Sort($comparer)
    return $list
}

# Gefilterte, sortierte Kinder eines Verzeichnisses liefern.
function Get-FilteredChildren {
    param([string]$Path)

    $gciParams = @{
        LiteralPath = $Path
        ErrorAction = 'SilentlyContinue'
    }
    if ($ShowAll) { $gciParams.Force = $true }

    $raw = Get-ChildItem @gciParams

    $filtered = foreach ($item in $raw) {
        if ($ExcludeDirs -contains $item.Name) { continue }

        if (-not $ShowAll) {
            if ($item.Name.StartsWith('.')) { continue }
            if ($item.Attributes -band [IO.FileAttributes]::Hidden) { continue }
        }
        $item
    }

    return @(Sort-SiblingsOrdinal -Items @($filtered))
}

# ----------------------------------------------------------------------
# 6.  Rekursiver Aufbau des Baums  (erzeugt die Zeilen direkt, in korrekter
#     Reihenfolge -> KEIN Nach-Sortieren mehr nötig!)
# ----------------------------------------------------------------------
function Build-Tree {
    param(
        [string]$Path,
        [string]$Prefix = '',
        [int]$Depth = 0
    )

    # Tiefenbegrenzung: sobald wir das erlaubte Level erreicht haben, nicht
    # weiter absteigen.
    if ($null -ne $MaxDepth -and $Depth -ge $MaxDepth) { return }

    $children = Get-FilteredChildren -Path $Path
    $count = $children.Count

    for ($i = 0; $i -lt $count; $i++) {
        $child  = $children[$i]
        $isLast = ($i -eq ($count - 1))

        $connector = if ($isLast) { $CONNECT_LAST } else { $CONNECT_MID }

        $name = $child.Name
        if ($child.PSIsContainer) { $name += '/' }

        $OutputLines.Add("$Prefix$connector$name")

        if ($child.PSIsContainer) {
            $childPrefix = if ($isLast) { "$Prefix$INDENT_LAST" } else { "$Prefix$INDENT_MID" }
            Build-Tree -Path $child.FullName -Prefix $childPrefix -Depth ($Depth + 1)
        }
    }
}

# ----------------------------------------------------------------------
# 7.  Ausführung
# ----------------------------------------------------------------------
$rootHeader = $RootPath.TrimEnd('\', '/') + '/'
$OutputLines.Add($rootHeader)

Build-Tree -Path $RootFull -Prefix '' -Depth 0

# ----------------------------------------------------------------------
# 8.  Ausgabe
# ----------------------------------------------------------------------
if ($OutFile) {
    # UTF-8 OHNE BOM, versions-unabhängig (5.1 schreibt bei -Encoding UTF8 sonst ein BOM).
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllLines($OutFile, $OutputLines, $utf8NoBom)
    [Console]::Error.WriteLine("OK: geschrieben nach $OutFile")
} else {
    foreach ($line in $OutputLines) { Write-Output $line }
}

exit 0