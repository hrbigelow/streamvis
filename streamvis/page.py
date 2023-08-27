import numpy as np
import re
from functools import partial
from panel.pane.holoviews import HoloViews 
import panel as pn
import holoviews as hv
import holoviews.plotting.bokeh
# from bokeh.layouts import column, row
from bokeh.models.dom import HTML
# from bokeh.models import Div, ColumnDataSource
# from bokeh.plotting import figure
from bokeh import palettes
from streamvis import util
import pdb

class IndexPage:
    """
    An index page, providing links to each available plot
    """
    def __init__(self, server, doc):
        self.server = server
        self.doc = doc
        self.session_id = doc.session_context.id
        self.doc.on_session_destroyed(self.destroy)

    def destroy(self, session_context):
        self.server.delete_page(self.session_id)

    def build_callback(self):
        page = pn.Column()
        head = pn.pane.Markdown('''# Streamvis Server Index Page''')
        page.append(head)

        for plot in self.server.schema.keys():
            item = pn.pane.HTML(f'<p>{plot}</p>')
            page.append(item)

        self.doc.add_root(page.get_root())

    def update(self):
        # no-op because the schema doesn't change
        pass

class PageLayout:
    """
    Represents a browser page
    """
    def __init__(self, server, doc):
        self.server = server
        # index into the server message log
        self.read_pos = 0
        self.doc = doc
        self.session_id = doc.session_context.id
        self.doc.on_session_destroyed(self.destroy)
        self.coords = None
        self.nbox = None
        self.box_elems = None
        self.widths = None
        self.heights = None
        self.last_ord = 0 # the last seen ord value

    def destroy(self, session_context):
        self.server.delete_page(self.session_id)

    def _set_layout(self, plots, box_elems, box_part, plot_part):
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
        self.plot_names = plots
        self.versions = [-1] * len(plots)
        self.box_elems = box_elems

        denom = sum(box_part)
        box_norm = []
        for part, nelem in zip(box_part, box_elems):
            box_norm.extend([part / denom] * nelem)

        self.nbox = len(box_elems)
        cumul = [0] + [ sum(box_elems[:i+1]) for i in range(self.nbox) ]

        def _sl(lens, i):
            return slice(cumul[i], cumul[i+1])

        slices = [ plot_part[_sl(box_elems, i)] for i in range(self.nbox) ]
        plot_norm = [ v / sum(sl) for sl in slices for v in sl ]

        # self.coords[i] = (box_index, elem_index) for plot i
        self.coords = []
        for box, sz in enumerate(box_elems):
            self.coords.extend([(box, elem) for elem in range(sz)])

        if self.row_mode:
            self.widths = plot_norm
            self.heights = box_norm
        else:
            self.widths = box_norm
            self.heights = plot_norm

    def process_request(self, request):
        """
        API:
        rows:   semi-colon separated list of csv names lists
        cols:   semi-colon separate list of csv names lists
        
        width:  csv numbers list
        height: csv numbers list

        Exactly one of `rows` or `cols` must be given.  Both `width` and `height` are
        optional.

        This function only accesses the server schema, not the data state
        """
        args = request.arguments
        known_plots = self.server.schema.keys()

        def maybe_get(args, param):
            val = args.pop(param, None)
            return val if val is None else val[0].decode()

        def parse_grid(param, grid, plots, box_elems):
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

        def parse_csv(param, arg, target_nelems):
            # Expect arg to be a csv numbers with target_nelems 
            if arg is None:
                return [1] * target_nelems
            try:
                num_list = list(map(float, arg.split(',')))
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

        rows = maybe_get(args, 'rows')
        cols = maybe_get(args, 'cols')
        if (rows is None) == (cols is None):
            raise RuntimeError(
                f'Exactly one of `rows` or `cols` query parameter must be given')

        plots = [] 
        box_elems = [] # 
        box_part = []  # box stacking dimension proportion 
        plot_part = [] # plot packing dimension proportions

        width_arg = maybe_get(args, 'width')
        height_arg = maybe_get(args, 'height')

        if rows is not None:
            self.row_mode = True
            parse_grid('rows', rows, plots, box_elems)
            plot_part = parse_csv('width', width_arg, len(plots))
            box_part = parse_csv('height', height_arg, len(box_elems))
        else:
            self.row_mode = False
            parse_grid('cols', cols, plots, box_elems)
            plot_part = parse_csv('height', height_arg, len(plots))
            box_part = parse_csv('width', width_arg, len(box_elems))
            
        self._set_layout(plots, box_elems, box_part, plot_part)

    def get_figsize(self, index):
        width = int(self.widths[index] * self.page_width)
        height = int(self.heights[index] * self.page_height)
        return dict(width=width, height=height)

    def set_pagesize(self, width, height):
        self.page_width = width
        self.page_height = height

    def build_callback(self):
        """
        The contents of the page are stored in `self.model`, which is either a
        pn.Column of pn.Rows or vice versa.  The elements of the inner container are
        hv.Overlays labeled with the `plot_name`s given in the URI
        """
        row_opts = dict(
                flex_direction='row',
                styles={'width': 'fit-content'},
                min_width=self.page_width)

        col_opts = dict( 
                flex_direction='column',
                styles={'height': 'fit-content'},
                min_height=self.page_height)

        opts = { True: row_opts, False: col_opts }

        self.model = pn.FlexBox(**opts[not self.row_mode])
        boxes = []
        if self.row_mode:
            main_parts, sub_parts = self.heights, self.widths
        else:
            main_parts, sub_parts = self.widths, self.heights

        for index, plot_name in enumerate(self.plot_names):
            box_index, _ = self.coords[index]
            if box_index >= len(boxes):
                style = { 'flex-grow': str(main_parts[index]) }
                these_opts = { **opts[self.row_mode] }
                these_opts['styles'].update(style)
                box = pn.FlexBox(**these_opts)
                boxes.append(box)
            box = boxes[box_index] 
            fig_kwargs = self.server.schema[plot_name].get('figure_kwargs', {})
            fig_kwargs.update(self.get_figsize(index))
            fig = hv.Overlay([], group='G', label=plot_name) # .opts(**fig_kwargs)
            fig = HoloViews(fig, styles={'flex-grow': str(sub_parts[index])})
            box.append(fig)
        self.model.extend(boxes)
        # self.doc.add_next_tick_callback(self.update)

    @staticmethod
    def matching_groups(plot_schema, groups):
        matched = []
        for g in groups:
            if (re.match(plot_schema['scope_pattern'], g.scope) and
                    re.match(plot_schema['group_pattern'], g.name)):
                matched.append(g)
        return matched

    @staticmethod
    def color(plot_schema, index):
        palette = plot_schema.get('palette', None)
        if palette is not None:
            return palettes.__dict__[palette][index]
        glyph_kwargs = plot_schema.get('glyph_kwargs', {})
        return glyph_kwargs.get('line_color', 'black')

    def validate_schema(self):
        pass

    def get_plot(self, name):
        fn = lambda l: isinstance(l, HoloViews) and l.object.label == name
        return self.model.select(fn)[0].object

    def update_element_cb(self, plot_schema, plot_name, group, new_data):
        """
        Possibly create a hv.Element (and associated hv.streams.Buffer)
        send new_data into the Buffer
        """
        olay = self.get_plot(plot_name)
        label = f'Group_{group.id}'
        line = olay.get('G.{label}')
        reload_model = False
        if line is None:
            buf = hv.streams.Buffer(np.zeros((0,2)), length=10000000)
            line = hv.DynamicMap(hv.Curve, streams=[buf], group='G', label=label)
            olay.update(hv.Overlay([line]))
            olay.collate()
            reload_model = True

        line.streams[0].send(new_data)
        if reload_model:
            self.doc.clear()
            self.doc.add_root(self.model.get_root())

    def update(self):
        """
        - Adds new plot data to existing curves
        - Creates new curves for any new data groups
        """
        print('in update')
        with self.server.update_lock:
            if self.last_ord == self.server.global_ordinal:
                print('returning early')
                return
            callback_fns = []
            for plot_name in self.plot_names:
                plot = self.get_plot(plot_name)
                groups = self.server.plot_groups[plot_name]
                plot_schema = self.server.schema[plot_name]
                for group in groups:
                    new_data = self.server.new_cds_data(group.id, self.last_ord + 1)
                    update_fn = partial(self.update_element_cb,
                            plot_schema, plot_name, group, new_data)
                    callback_fns.append(update_fn)
            self.last_ord = self.server.global_ordinal

        for fn in callback_fns:
            self.doc.add_next_tick_callback(fn)
        print('finished update')


