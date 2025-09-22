import re
from typing import Any
from bokeh.layouts import column, row
from bokeh.model.model import Model
from bokeh.application.application import SessionContext
import grpc
from grpc import aio
from . import util
from .base import BasePage

def parse_csv(param, arg, target_nelems):
    # returns a num_list representing the csv param
    if arg is None:
        return [1] * target_nelems
    try:
        num_list = [float(v) for v in arg.split(",")]
    except ValueError:
        raise RuntimeError(
            f'{param} value \'{arg}\' is not a valid csv list of numbers')
    if any(num <= 0 for num in num_list):
        raise RuntimeError(
            f'{param} value \'{arg}\' are not all positive numbers')
    if len(num_list) != target_nelems:
        raise RuntimeError(
            f'Received {len(num_list)} values but expected {target_nelems}. '
            f'Context: {param}={arg}')
    return num_list 


def parse_grid(known_plots, param, grid, plots, box_elems):
    # modifies plots and box_elems
    if grid == '':
        raise RuntimeError(f'Got empty {param} value') 
    blocks = grid.split(';')
    for block in blocks:
        items = block.split(',')
        for plot in items:
            if plot not in known_plots:
                raise RuntimeError(
                    f'In {param}={grid}, plot \'{plot}\' is not in the schema. '
                    f'Schema contains plots {", ".join(known_plots)}')
            plots.append(plot)
        box_elems.append(len(items))


def get_decode(args, param):
    vals = args.get(param)
    if vals is None:
        return None
    return tuple(v.decode() for v in vals)


class PageLayout(BasePage):
    """Represents a browser page."""
    def __init__(self, server):
        super().__init__(server)

    def _set_layout(self, box_elems, box_part, plot_part, args) -> dict[str, Any]:
        """
        The overall page layout is a list of 'boxes', with each box containing
        one or more plots.  The boxes are either all Bokeh row or column objects.
        Accordingly, each plot is addressed by a (box, elem) tuple indicating which
        box it is in, and its position within the box.

        Computes height and width fractions for all plots and sets the properties.
        `widths` and `heights`.  Once overall page width and height are known, these
        fractions can be used to resolve the individual plot dimensions in pixels.

        box_elems:  box_elems[i] = number of plots in box i
        box_part:   box_part[i] = relative stacking size of box i
        plot_part:  plot_part[i] = relative size of plot i

        """
        denom = sum(box_part)
        box_norm = []
        for part, nelem in zip(box_part, box_elems):
            box_norm.extend([part / denom] * nelem)

        args["box-norm"] = box_norm

        nbox = len(box_elems)
        cumul = [0] + [ sum(box_elems[:i+1]) for i in range(nbox) ]

        def _sl(lens, i):
            return slice(cumul[i], cumul[i+1])

        slices = [ plot_part[_sl(box_elems, i)] for i in range(nbox) ]
        plot_norm = [ v / sum(sl) for sl in slices for v in sl ]

        args["coords"] = []
        for box, sz in enumerate(box_elems):
            args["coords"].extend([(box, elem) for elem in range(sz)])

        if args["row-mode"]:
            args["widths"] = plot_norm
            args["heights"] = box_norm
        else:
            args["widths"] = box_norm
            args["heights"] = plot_norm

    def process_request(self, request) -> dict[str, Any]:
        """
        API:
        scopes: regex pattern of scopes to include 
        rows:   semi-colon separated list of csv plot names lists
        cols:   semi-colon separated list of csv plot names lists
        names:  regex pattern of names to include (use one query param per plot name,
                in same order as the names in rows or cols is given)
        xlog:   if present, set x axis to log-scale
        ylog:   if present, set y axis to log-scale
        
        width:  csv numbers list
        height: csv numbers list

        Exactly one of `rows` or `cols` must be given.  Both `width` and `height` are
        optional.

        scopes is optional.  if absent, defaults to '.+'
        names is optional.  if absent, defaults to '.+'

        This function only accesses the server schema, not the data state
        """
        # import pdb
        # pdb.set_trace()
        out_args = {}
        args = request.arguments
        known_plots = self.server.schema.keys()

        scope_pats = get_decode(args, "scopes") 
        if scope_pats is None:
            scope_pats = (".*",)
        if len(scope_pats) != 1:
            raise RuntimeError(f"scopes argument must be provided exactly once")

        scope_pat = scope_pats[0]
        out_args["scope-pat"] = scope_pat
        try:
            re.compile(scope_pat)
        except re.PatternError as ex:
            raise RuntimeError(f"scopes argument '{scope_pat}' is not a valid regex")

        rows = get_decode(args, "rows")
        cols = get_decode(args, "cols")
        if rows is not None and cols is None and len(rows) == 1:
            rows = rows[0]
        elif cols is not None and rows is None and len(cols) == 1:
            cols = cols[0]
        else:
            raise RuntimeError(
                f"Either `rows` or `cols` query parameter must be given exactly once")

        name_pats = get_decode(args, "names")

        plots = [] 
        box_elems = [] # 
        box_part = []  # box stacking dimension proportion 
        plot_part = [] # plot packing dimension proportions

        width_arg = get_decode(args, 'width')
        height_arg = get_decode(args, 'height')

        if rows is not None:
            out_args["row-mode"] = True
            parse_grid(known_plots, 'rows', rows, plots, box_elems)
            plot_part = parse_csv('width', width_arg, len(plots))
            box_part = parse_csv('height', height_arg, len(box_elems))
        else:
            out_args["row-mode"] = False
            parse_grid(known_plots, 'cols', cols, plots, box_elems)
            plot_part = parse_csv('height', height_arg, len(plots))
            box_part = parse_csv('width', width_arg, len(box_elems))
            
        if len(plots) != len(name_pats):
            raise RuntimeError(
                f"Must provide same number of names as plots in `cols` or `rows`.  "
                f"Received {len(plots)} plots and {len(name_pats)} names query parameters")

        try:
            _ = tuple(re.compile(n) for n in name_pats)
        except re.PatternError as ex:
            raise RuntimeError(
                    f"Error compiling one or more of names arguments: {name_pats}: {ex}")

        out_args["name-pats"] = tuple(name_pats)
        out_args["plots"] = tuple(plots)

        axes_arg = get_decode(args, "axes")
        if axes_arg is None:
            axes = ("lin",) * len(plots) 
        elif len(axes_arg) == 1:
            axes = axes_arg[0].split(",")
        else:
            raise RuntimeError(f"`axes` query parameter must be provided at most once")
        if (len(axes) != len(plots) 
            or any(mode not in ("lin", "xlog", "ylog", "xylog") for mode in axes)):
            raise RuntimeError(
                f"`axes` must be comma-separated list of modes, one for each plot.  "
                f"Each mode should be one of 'lin', 'xlog', 'ylog', 'xylog'.  "
                f"Received {axes_arg=}")
        out_args["axes-modes"] = tuple(axes)

        self._set_layout(box_elems, box_part, plot_part, out_args) # update out_args
        return out_args 
        

    def build_page(self, ctx: SessionContext, page_width: int, page_height: int) -> Model:
        """Build actual page content after screen size is known."""
        row_mode = ctx.token_payload.get("row-mode")
        coords = ctx.token_payload.get("coords")
        model = column() if row_mode else row() 
        session_state = ctx.custom_state

        for index, plot in enumerate(session_state.plots):
            # print(f"{session_state.id}: {plot.name}")
            box_index, _ = coords[index]
            if box_index >= len(model.children):
                box = row() if row_mode else column()
                model.children.append(box)
            box = model.children[box_index]
            plot_model = plot.build(ctx.token_payload, page_width, page_height)
            box.children.append(plot_model)
            # print(f'in build, appended {fig=}, {fig.height=}, {fig.width=}, {fig.title=}')
        return model

