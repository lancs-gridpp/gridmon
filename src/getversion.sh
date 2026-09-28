#!/bin/bash
# -*- c-basic-offset: 4; indent-tabs-mode: nil -*-

## Parse command-line arguments.
FILENAME=()
while [ $# -gt 0 ] ; do
    arg="$1" ; shift
    case "$arg" in
        (--prefix=*)
            prefix="${arg#--prefix=}"
            ;;

        (-*|+*)
            printf >&2 '%s: unknown switch: %s\n' "$0" "$arg"
            exit 1
            ;;

        (*)
            FILENAME+=("$arg")
            ;;
    esac
done

## Get the most recent tag as the release, and strip off any prefix.
release="$(git describe 2> /dev/null)"
release="${release#"$prefix"}"

## Extract the number of commits since the tag, and the latest commit.
if [[ "$release" =~ ^(.*)-([0-9]+)-g([0-9a-f]+)$ ]] ; then
    release="${BASH_REMATCH[1]}"
    gitadv="${BASH_REMATCH[2]}"
    gitrev="${BASH_REMATCH[3]}"
fi

## Detect untracked files or uncommitted changes.
if git diff-index --quiet HEAD ; then
    unset alpha
else
    ## There's something to commit.
    alpha='a'
fi

unset longext
digits='[0-9]+'
expr=""

for i in "${!FILENAME[@]}" ; do
    fn="${FILENAME[i]}"
    re='^('"$expr"')'"${expr:+\.}"'('"$digits"')'
    if [ -n "$fn" -a -r "$fn" ] && [[ "$release" =~ $re ]] ; then
        longext="$alpha"
        result="${BASH_REMATCH[1]}${BASH_REMATCH[1]:+.}$((BASH_REMATCH[2]+1))"
        ((i++))
        while (( i < ${#FILENAME[@]} )) ; do
            result+=".0"
            ((i++))
        done
        break
    fi

    expr+="${expr:+\.}${digits}"
done
re='^('"$expr"')'
if [ -z "$result" -a -n "$gitadv" ] && [[ "$release" =~ $re ]] ; then
    longext=".$((gitadv + 1))$alpha"
    result="${BASH_REMATCH[1]}"
fi
if [ -z "$result" ] ; then
    result="$release"
fi

longrelease="${result}$longext${gitrev:+-g"$gitrev"}"
printf '%s %s\n' "$result" "$longrelease"
